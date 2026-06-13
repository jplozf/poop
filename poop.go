package main

// ****************************************************************************
// IMPORTS
// ****************************************************************************
import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	util "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/multiformats/go-multiaddr"
	"github.com/rivo/tview"
)

// ****************************************************************************
// TYPES
// ****************************************************************************
type AppState int

// ****************************************************************************
// CONSTS
// ****************************************************************************
const (
	defaultBootstrapPort          = 42001
	protocolID                    = "/poop/sync/1.0.0"
	fileProtocolID                = "/poop/file/1.0.0"
	StateIdle            AppState = iota
	StateAwaitingAuth             // We received a request, waiting for user to type 'y' or 'n'
	StateInSession                // We are actively talking to someone
)

// ****************************************************************************
// VARS
// ****************************************************************************
var (
	appName              = "poop"
	appURL               = "https://github.com/jplozf/poop"
	appMajorVersion      = "0"
	GitVersion           = "dev"
	previousStatus       AppState
	previousActivePeerID peer.ID
	currentStatus        = StateIdle
	// sessions stores active chat sessions.
	// Each entry contains the network.Stream and its associated bufio.ReadWriter
	// to ensure consistent buffering and avoid read/write conflicts.
	sessions = make(map[peer.ID]struct {
		Stream network.Stream
		RW     *bufio.ReadWriter
	})
	activePeerID   peer.ID                    // The peer currently shown in the chat window
	sessionBuffers = make(map[peer.ID]string) // Stores history for background chats
	peerAliases    = make(map[peer.ID]string) // Stores aliases for peers
	myAlias        string                     // Our global alias

	// Specifically for the auth flow
	authStream network.Stream
	authChan   chan string // Used to communicate choice and alias to the handler
	sessionMu  sync.RWMutex

	configPath  string
	incomingDir string
	historyFile = filepath.Join(os.TempDir(), ".poop_history")
	ctx         = context.Background()
	h           host.Host
	kademliaDHT *dht.IpfsDHT

	// UI Components
	app             *tview.Application
	commandView     *tview.TextView
	chatView        *tview.TextView
	sessionListView *tview.TextView
	inputField      *tview.InputField
	history         []string
	historyIdx      = -1

	discoveredPeers []peer.ID
	peerMu          sync.Mutex
)

func registerPeer(id peer.ID) int {
	peerMu.Lock()
	defer peerMu.Unlock()
	for i, p := range discoveredPeers {
		if p == id {
			return i + 1
		}
	}
	discoveredPeers = append(discoveredPeers, id)
	return len(discoveredPeers)
}

func resolveCommand(input string, commands []string) (string, error) {
	var matches []string
	for _, cmd := range commands {
		if cmd == input {
			return cmd, nil // Exact match always wins
		}
		if strings.HasPrefix(cmd, input) {
			matches = append(matches, cmd)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("ambiguous command '%s': could be %s", input, strings.Join(matches, ", "))
	}
	return "", fmt.Errorf("unknown command '%s'", input)
}

// discoveryNotifee gets notified when we find a new peer via mDNS
type discoveryNotifee struct {
	h host.Host
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if app == nil {
		return
	}

	idx := registerPeer(pi.ID)
	app.QueueUpdateDraw(func() {
		fmt.Fprintf(commandView, "[green][Discovery][-]: Found peer [$%d] %s with %d addresses\n", idx, pi.ID, len(pi.Addrs))
	})
	// Optional: Automatically connect to them
	if err := n.h.Connect(context.Background(), pi); err != nil {
		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "[red]Connection to discovered peer failed: %s[-]\n", err)
		})
	}
}

// ****************************************************************************
// main()
// ****************************************************************************
func main() {
	isServer := flag.Bool("s", false, "run as a bootstrap server (headless)")
	flag.Parse()

	if *isServer {
		runBootstrapServer()
		return
	}

	// 1. Initialize UI
	app = tview.NewApplication()

	commandView = tview.NewTextView().
		SetDynamicColors(true).
		SetRegions(true).
		SetWordWrap(true).
		SetChangedFunc(func() {
			commandView.ScrollToEnd()
		})
	commandView.SetBorder(true).SetTitle(" System & Commands ")

	chatView = tview.NewTextView().
		SetDynamicColors(true).
		SetWordWrap(true).
		SetChangedFunc(func() {
			chatView.ScrollToEnd()
		})
	chatView.SetBorder(true).SetTitle(" Chat Session ")

	sessionListView = tview.NewTextView().
		SetDynamicColors(true).
		SetWordWrap(true)
	sessionListView.SetBorder(true).SetTitle(" Active Sessions ")

	inputField = tview.NewInputField().
		SetLabel("> ").
		SetFieldWidth(0)

	// Initialize directories
	homeDir, erd := os.UserHomeDir()
	if erd != nil {
		panic(fmt.Sprintf("Failed to get user home directory: %v", erd))
	}
	incomingDir = filepath.Join(homeDir, ".poop", "incoming")
	os.MkdirAll(incomingDir, 0755)

	mrand.Seed(time.Now().UnixNano())

	// Try to load alias from config.json
	configPath = filepath.Join(homeDir, ".poop", "config.json")
	if data, err := os.ReadFile(configPath); err == nil {
		var cfg struct {
			Alias string `json:"alias"`
		}
		if err := json.Unmarshal(data, &cfg); err == nil && cfg.Alias != "" {
			myAlias = cfg.Alias
		}
	}

	if myAlias == "" {
		myAlias = generateRandomAlias()
	}
	chatView.SetTitle(fmt.Sprintf(" Chat Session (as %s) ", myAlias))

	setupInputHandlers()

	// Layout: Left column (Command | Sessions), Right column (Chat), Bottom row (Input)
	leftFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(commandView, 0, 2, false).
		AddItem(sessionListView, 0, 1, false)

	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(tview.NewFlex().
			AddItem(leftFlex, 0, 1, false).
			AddItem(chatView, 0, 1, false), 0, 1, false).
		AddItem(inputField, 1, 1, true)

	fullVersion := fmt.Sprintf("%s.%s", appMajorVersion, GitVersion)
	fmt.Fprintf(commandView, "[yellow]Welcome to Poop v%s[-]\n", fullVersion)
	fmt.Fprintf(commandView, "Please, type 'help' for info.\n")
	go checkForUpdates()

	// 1. Create the Libp2p Host with NAT traversal capabilities
	var err error
	h, err = libp2p.New(
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",         // Regular TCP
			"/ip4/0.0.0.0/udp/0/quic-v1", // QUIC-v1 for better hole punching
		),
		libp2p.NATPortMap(),         // Attempt to open ports via UPnP/NAT-PMP
		libp2p.EnableRelay(),        // Allows this node to use relays to reach others
		libp2p.EnableHolePunching(), // Enables DCUtR hole punching
	)
	if err != nil {
		panic(err)
	}
	defer h.Close()

	// 1.3 Setup Global DHT discovery
	kademliaDHT, err = dht.New(ctx, h)
	if err != nil {
		panic(err)
	}
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		panic(err)
	}

	// 1.2 setup local mDNS discovery
	ser := mdns.NewMdnsService(h, "poop-p2p-discovery", &discoveryNotifee{h: h})
	if err := ser.Start(); err != nil {
		fmt.Fprintf(commandView, "[red]Failed to start mDNS: %s[-]\n", err)
	}
	defer ser.Close()

	// 1.5 Connect to bootstrap nodes to find relays and discover our external IP
	bootstrapNodes := []string{
		"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvLcZunBNqv9U7Zkx6n6TVv4N497Xp9EWiZfWob",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooP7bfuAGnS2V1qSEpT6B9W5itW39pVRJ3f7qXmSSW",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcSTwsNBVB6nxGbtTVTSpx67uYv9SStm6D8Cq3H2",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMo9iavMyaH7YvUXdf22Qx8Qo3AAn2CyF7C2QByq",
	}

	for _, addrStr := range bootstrapNodes {
		ma, err := multiaddr.NewMultiaddr(strings.TrimSpace(addrStr))
		if err != nil {
			fmt.Fprintf(commandView, "[red]Error parsing bootstrap addr: %s[-]\n", err)
			continue
		}
		peerinfo, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			fmt.Fprintf(commandView, "[red]Error getting peer info: %s[-]\n", err)
			continue
		}
		h.Connect(ctx, *peerinfo)
	}

	// 1.6 Start global discovery in a default room
	// In a real app, you might want to let the user choose this
	defaultRoom := "poop-p2p-global-default"
	go discoverPeers(ctx, h, kademliaDHT, defaultRoom)

	// 2. Setup the Listener (Receiver) logic
	h.SetStreamHandler(protocolID, func(s network.Stream) {
		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "[green][Incoming] New message from %s[-]\n", s.Conn().RemotePeer())
		})
		rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))

		str, _ := rw.ReadString('\n')
		idx := registerPeer(s.Conn().RemotePeer())
		content := strings.TrimSpace(str)
		app.QueueUpdateDraw(func() {
			fmt.Fprintf(chatView, "[yellow]%s[-]: %s\n", tview.Escape(fmt.Sprintf("[$%d>me]", idx)), content)
		})
		s.Close()
	})

	h.SetStreamHandler(fileProtocolID, func(s network.Stream) {
		remotePeer := s.Conn().RemotePeer()
		idx := registerPeer(remotePeer)

		reader := bufio.NewReader(s)
		fileName, _ := reader.ReadString('\n')
		fileName = strings.TrimSpace(fileName)
		sizeStr, _ := reader.ReadString('\n')
		size, _ := strconv.ParseInt(strings.TrimSpace(sizeStr), 10, 64)

		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "[yellow][File] Receiving '%s' (%d bytes) from [$%d]...[-]\n", fileName, size, idx)
		})

		// Construct the full path for the incoming file
		fullPath := filepath.Join(incomingDir, fileName)

		// Check if a file with the same name already exists and append a number if it does
		for i := 1; fileExists(fullPath); i++ {
			fullPath = filepath.Join(incomingDir, fmt.Sprintf("%s_%d%s", strings.TrimSuffix(fileName, filepath.Ext(fileName)), i, filepath.Ext(fileName)))
		}
		out, err := os.Create(fullPath)
		if err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(commandView, "[red]Failed to create file: %s[-]\n", err)
			})
			s.Reset()
			return
		}
		defer out.Close()

		_, err = io.CopyN(out, reader, size)
		if err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(commandView, "[red]File transfer failed: %s[-]\n", err)
			})
		} else {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(commandView, "[green][File] '%s' saved successfully to %s[-]\n", fileName, fullPath)
			})
		}
		s.Close()
	})

	h.SetStreamHandler("/poop/auth/1.0.0", func(s network.Stream) {
		// Create a buffered reader/writer for the incoming stream
		rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
		remoteID := s.Conn().RemotePeer()
		idx := registerPeer(remoteID)

		// Read the initial message from the initiator to unblock them
		initialMsg, err := rw.ReadString('\n')
		if err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(commandView, "[red]Error reading initial auth message from %s: %s[-]\n", s.Conn().RemotePeer(), err)
			})
			s.Close()
			return
		}

		parts := strings.SplitN(strings.TrimSpace(initialMsg), " ", 2)
		if parts[0] != "SESSION_REQUEST" {
			rw.WriteString("REJECT_BAD_PROTOCOL\n")
			rw.Flush()
			s.Close()
			return
		}

		remoteAlias := "Unknown"
		if len(parts) > 1 {
			remoteAlias = parts[1]
		}

		sessionMu.Lock()
		if authStream != nil { // Another auth is already pending
			sessionMu.Unlock()
			s.Reset()
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(commandView, "[yellow][Discovery] Busy: rejected incoming session from %s[-]\n", remoteID)
			})
			rw.WriteString("REJECT_BUSY\n") // Use rw to send rejection
			rw.Flush()
			s.Close()
			return
		}
		// Store current state before changing to AwaitingAuth
		previousStatus = currentStatus
		previousActivePeerID = activePeerID
		authStream = s
		currentStatus = StateAwaitingAuth
		authChan = make(chan string, 2) // Buffered to prevent blocking the UI thread
		sessionMu.Unlock()

		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "[yellow][!!!] Incoming session from '%s' (as $%d) %s[-]\n", remoteAlias, idx, remoteID)
			inputField.SetLabel("Accept incoming? (y/n): ")
		})

		// Wait for user input from the main UI thread via authChan
		signal, ok := <-authChan
		if !ok || signal != "Y" {
			rw.WriteString("REJECT\n")
			rw.Flush()
			s.Close()

			sessionMu.Lock()
			currentStatus = previousStatus
			activePeerID = previousActivePeerID
			sessionMu.Unlock()

			app.QueueUpdateDraw(func() {
				fmt.Fprintln(commandView, "[yellow]Incoming connection declined.[-]")
				restoreUILabel()
			})
			go updateSessionList()
			return
		}

		sessionMu.Lock()
		authChan = nil // Clear early to prevent UI thread from blocking on double-input
		authStream = nil
		sessionMu.Unlock()

		// User accepted: Send ACK with our global alias
		rw.WriteString(fmt.Sprintf("ACK %s\n", myAlias))
		rw.Flush()

		sessionMu.Lock()
		peerAliases[remoteID] = remoteAlias
		sessions[remoteID] = struct {
			Stream network.Stream
			RW     *bufio.ReadWriter
		}{Stream: s, RW: rw}
		activePeerID = remoteID
		currentStatus = StateInSession
		sessionMu.Unlock()

		app.QueueUpdateDraw(func() {
			inputField.SetLabel(fmt.Sprintf("[Chatting with $%d]: ", idx))
			fmt.Fprintf(commandView, "[green]--- Session Started with %s ($%d) ---[-]\n", remoteAlias, idx)
			chatView.SetText("")
		})
		go updateSessionList()
		runChatLoop(remoteID)
	})

	// 3. Print local info so others can connect to us
	fmt.Fprintf(commandView, "Your Peer ID: [white]%s[-]\n", h.ID())
	fmt.Fprintln(commandView, "System starting... type 'status' to see public addresses.")

	for _, addr := range h.Addrs() {
		fmt.Fprintf(commandView, "Local Addr: %s/p2p/%s\n", addr, h.ID())
	}

	if err := app.SetRoot(mainFlex, true).Run(); err != nil {
		panic(err)
	}
}

func setupInputHandlers() {
	inputField.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyUp:
			if len(history) > 0 {
				if historyIdx == -1 {
					historyIdx = len(history) - 1
				} else if historyIdx > 0 {
					historyIdx--
				}
				inputField.SetText(history[historyIdx])
			}
			return nil
		case tcell.KeyDown:
			if historyIdx != -1 {
				if historyIdx < len(history)-1 {
					historyIdx++
					inputField.SetText(history[historyIdx])
				} else {
					historyIdx = -1
					inputField.SetText("")
				}
			}
			return nil
		}
		return event
	})

	inputField.SetDoneFunc(func(key tcell.Key) {
		if key != tcell.KeyEnter {
			return
		}
		line := strings.TrimSpace(inputField.GetText())

		// Add to history if it's not empty and different from the last entry
		if len(line) > 0 {
			if len(history) == 0 || history[len(history)-1] != line {
				history = append(history, line)
			}
		}
		historyIdx = -1
		inputField.SetText("")

		sessionMu.RLock()
		status := currentStatus
		sessionMu.RUnlock()

		// Allow empty lines in auth states to prevent the UI from getting "stuck"
		// if the user just hits Enter.
		if len(line) == 0 && status != StateAwaitingAuth {
			return
		}

		switch status {
		case StateAwaitingAuth:
			handleAuthInput(line)
		case StateInSession:
			if len(line) > 0 {
				handleSessionInput(line)
			}
		case StateIdle:
			if len(line) > 0 {
				processedLine := line
				if strings.HasPrefix(line, "/") {
					processedLine = strings.TrimPrefix(line, "/")
				}
				handleCommandInput(processedLine, h)
			}
		}
	})
}

// ****************************************************************************
// startSession()
// ****************************************************************************
func startSession(ctx context.Context, h host.Host, target string) {
	// Run the connection logic in a goroutine to avoid freezing the UI thread
	go func() {
		var info *peer.AddrInfo

		// 1. Try to parse as a full Multiaddr first
		if maddr, err := multiaddr.NewMultiaddr(target); err == nil {
			info, err = peer.AddrInfoFromP2pAddr(maddr)
			if err != nil {
				fmt.Fprintf(commandView, "[red]Could not get peer info from multiaddr: %s[-]\n", err)
				return
			}
		} else {
			// 2. If it's not a multiaddr, try parsing as a Peer ID string
			id, err := peer.Decode(target)
			if err != nil {
				app.QueueUpdateDraw(func() {
					fmt.Fprintf(commandView, "[red]Input is not a valid Multiaddr or Peer ID: %s[-]\n", err)
				})
				return
			}

			knownAddrs := h.Peerstore().Addrs(id)
			if len(knownAddrs) == 0 {
				app.QueueUpdateDraw(func() {
					fmt.Fprintf(commandView, "[red]No known addresses for peer %s. Try using a full multiaddr.[-]\n", id)
				})
				return
			}
			info = &peer.AddrInfo{ID: id, Addrs: knownAddrs}
		}

		// 3. Connect to the peer
		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "Attempting to connect to %s...\n", info.ID)
		})

		if err := h.Connect(ctx, *info); err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(commandView, "[red]Connection failed: %s[-]\n", err)
			})
			return
		}

		// 4. Open the 'Poop' protocol stream
		s, err := h.NewStream(ctx, info.ID, "/poop/auth/1.0.0")
		if err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(commandView, "[red]Protocol error: %s[-]\n", err)
			})
			return
		}

		rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
		idx := registerPeer(info.ID)
		app.QueueUpdateDraw(func() {
			fmt.Fprintln(commandView, "Waiting for peer to accept the session...")
		})

		// We send a tiny 'knock' message
		rw.WriteString(fmt.Sprintf("SESSION_REQUEST %s\n", myAlias))
		rw.Flush()

		reply, err := rw.ReadString('\n')
		if err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintln(commandView, "[red]Peer closed the connection.[-]")
			})
			s.Close()
			return
		}

		reply = strings.TrimSpace(reply)
		if strings.HasPrefix(reply, "ACK") {
			alias := ""
			parts := strings.SplitN(reply, " ", 2)
			if len(parts) > 1 {
				alias = parts[1]
			}

			app.QueueUpdateDraw(func() {
				fmt.Fprintln(commandView, "[green]Success! Peer accepted the session.[-]")
				sessionMu.Lock()
				if alias != "" {
					peerAliases[info.ID] = alias
				}
				sessions[info.ID] = struct {
					Stream network.Stream
					RW     *bufio.ReadWriter
				}{Stream: s, RW: rw}
				activePeerID = info.ID
				currentStatus = StateInSession
				sessionMu.Unlock()
				inputField.SetLabel(fmt.Sprintf("[Chatting with $%d]: ", idx))
			})
			go updateSessionList()
			runChatLoop(info.ID) // Pass peer ID
		} else {
			app.QueueUpdateDraw(func() {
				fmt.Fprintln(commandView, "[red]Peer rejected the session request.[-]")
			})
			s.Close()
		}
	}()
}

// ****************************************************************************
// handleCommandInput()
// ****************************************************************************
func handleCommandInput(input string, h host.Host) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return
	}

	available := []string{"connect", "room", "peers", "status", "help", "id", "exit", "quit", "bye", "bootstrap", "send", "chat", "alias", "unset"}
	resolved, err := resolveCommand(parts[0], available)
	if err != nil {
		fmt.Fprintf(commandView, "[red]%s[-]. Type 'help' for info.\n", err)
		return
	}

	switch resolved {
	case "connect":
		if len(parts) < 2 {
			fmt.Fprintln(commandView, "Usage: connect <multiaddr_or_peerID_or_$index> (e.g., connect $1)")
			return
		}
		targetAddr := parts[1]

		if strings.HasPrefix(targetAddr, "$") {
			idx, err := strconv.Atoi(strings.TrimPrefix(targetAddr, "$"))
			peerMu.Lock()
			if err == nil && idx > 0 && idx <= len(discoveredPeers) {
				targetAddr = discoveredPeers[idx-1].String()
			} else {
				fmt.Fprintf(commandView, "[red]Invalid or unknown peer index: %s[-]\n", targetAddr)
				peerMu.Unlock()
				return
			}
			peerMu.Unlock()
		}

		startSession(context.Background(), h, targetAddr)

	case "chat":
		if len(parts) < 2 {
			fmt.Fprintln(commandView, "Usage: chat <$index>")
			return
		}
		idx, err := strconv.Atoi(strings.TrimPrefix(parts[1], "$"))
		if err != nil || idx <= 0 {
			fmt.Fprintln(commandView, "[red]Invalid index.[-]")
			return
		}

		peerMu.Lock()
		if idx > len(discoveredPeers) {
			fmt.Fprintln(commandView, "[red]Peer not found.[-]")
			peerMu.Unlock()
			return
		}
		targetID := discoveredPeers[idx-1]
		peerMu.Unlock()

		var success bool
		sessionMu.RLock()
		if _, ok := sessions[targetID]; ok {
			activePeerID = targetID
			currentStatus = StateInSession
			inputField.SetLabel(fmt.Sprintf("[Chatting with $%d]: ", idx))
			chatView.SetText(sessionBuffers[targetID])
			success = true
		} else {
			fmt.Fprintf(commandView, "[red]No active session with $%d. Use 'connect' first.[-]\n", idx)
		}
		sessionMu.RUnlock()

		if success {
			go updateSessionList()
		}

	case "room":
		if len(parts) < 2 {
			fmt.Fprintln(commandView, "Usage: room <name>")
			return
		}
		fmt.Fprintf(commandView, "[yellow]Joining discovery room: %s[-]\n", parts[1])
		go discoverPeers(ctx, h, kademliaDHT, parts[1])

	case "peers":
		fmt.Fprintln(commandView, "[yellow]Discovered Peer Shortcuts:[-]")
		peerMu.Lock()
		for i, id := range discoveredPeers {
			fmt.Fprintf(commandView, " [$%d] %s\n", i+1, id)
		}
		peerMu.Unlock()

	case "status":
		fmt.Fprintln(commandView, "[yellow]Current Addresses:[-]")
		for _, addr := range h.Addrs() {
			fmt.Fprintf(commandView, " - %s/p2p/%s\n", addr, h.ID())
		}

	case "bootstrap":
		if len(parts) < 2 {
			fmt.Fprintln(commandView, "Usage: bootstrap <multiaddr>")
			return
		}
		addrStr := parts[1]
		ma, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			fmt.Fprintf(commandView, "[red]Error parsing bootstrap addr: %s[-]\n", err)
			return
		}
		peerinfo, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			fmt.Fprintf(commandView, "[red]Error getting peer info: %s[-]\n", err)
			return
		}

		go func() {
			if err := h.Connect(ctx, *peerinfo); err != nil {
				app.QueueUpdateDraw(func() { fmt.Fprintf(commandView, "[red]Failed to connect to bootstrap node: %s[-]\n", err) })
			} else {
				app.QueueUpdateDraw(func() { fmt.Fprintf(commandView, "[green]Connected to bootstrap node: %s[-]\n", peerinfo.ID) })
				kademliaDHT.Bootstrap(ctx)
			}
		}()

	case "send":
		if currentStatus != StateInSession {
			fmt.Fprintln(commandView, "[red]You must be in an active session to send files.[-]")
			return
		}
		if len(parts) < 2 {
			fmt.Fprintln(commandView, "Usage: /send <filepath>")
			return
		}
		sessionMu.RLock()
		if _, ok := sessions[activePeerID]; ok {
			go sendFile(activePeerID, parts[1])
		}
		sessionMu.RUnlock()

	case "help":
		fmt.Fprintln(commandView, "Available commands: connect <addr>")
		fmt.Fprintln(commandView, "                    room <name>")
		fmt.Fprintln(commandView, "                    bootstrap <addr>")
		fmt.Fprintln(commandView, "                    send <path>")
		fmt.Fprintln(commandView, "                    alias <name>")
		fmt.Fprintln(commandView, "                    unset alias")
		fmt.Fprintln(commandView, "                    id")
		fmt.Fprintln(commandView, "                    peers")
		fmt.Fprintln(commandView, "                    status")
		fmt.Fprintln(commandView, "                    exit, quit, bye")
		fmt.Fprintln(commandView, "                    help")

		fmt.Fprintln(commandView, "These commands should be preceded by a '/' when in chat mode.")
		fmt.Fprintln(commandView, "These commands could be shortened as long as they are unambiguous.")

	case "id":
		fmt.Fprintf(commandView, "[yellow]Share this address with your peer:[-]\n")
		addr := h.Addrs()[0] // Use the first available addr
		fmt.Fprintf(commandView, "[white]%s/p2p/%s[-]\n", addr, h.ID())

	case "alias":
		if len(parts) < 2 {
			fmt.Fprintln(commandView, "Usage: alias <name>")
			return
		}
		newAlias := parts[1]
		myAlias = newAlias
		chatView.SetTitle(fmt.Sprintf(" Chat Session (as %s) ", myAlias))
		if err := saveAliasConfig(newAlias); err != nil {
			fmt.Fprintf(commandView, "[red]Failed to save alias: %s[-]\n", err)
		} else {
			fmt.Fprintf(commandView, "[green]Alias updated to: %s[-]\n", newAlias)
		}

	case "unset":
		if len(parts) < 2 || parts[1] != "alias" {
			fmt.Fprintln(commandView, "Usage: unset alias")
			return
		}
		myAlias = generateRandomAlias()
		chatView.SetTitle(fmt.Sprintf(" Chat Session (as %s) ", myAlias))
		if err := os.Remove(configPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(commandView, "[red]Failed to remove config: %s[-]\n", err)
		} else {
			fmt.Fprintf(commandView, "[green]Alias unset. Reset to random: %s[-]\n", myAlias)
		}

	case "exit", "quit", "bye":
		app.Stop()

	default:
		fmt.Fprintf(commandView, "[red]Unknown command: %s.[-] Type 'help' for info.\n", parts[0])
	}
}

// ****************************************************************************
// handleSessionInput()
// ****************************************************************************
func handleSessionInput(input string) {
	// Check if the input is a command (starts with /)
	if strings.HasPrefix(input, "/") {
		cmdStr := strings.TrimPrefix(input, "/")
		parts := strings.Fields(cmdStr)
		if len(parts) == 0 {
			return
		}

		available := []string{"connect", "room", "peers", "status", "help", "id", "exit", "quit", "bye", "bootstrap", "send", "chat", "alias", "unset"}
		resolved, err := resolveCommand(parts[0], available)
		if err != nil {
			fmt.Fprintf(commandView, "[red]%s[-]\n", err)
			return
		}

		if resolved == "quit" {
			sessionMu.Lock()
			if sessionEntry, ok := sessions[activePeerID]; ok {
				sessionEntry.Stream.Close() // Close the stream
				delete(sessions, activePeerID)
			}
			sessionMu.Unlock()

			fmt.Fprintln(commandView, "Session closed.")
			currentStatus = StateIdle
			inputField.SetLabel("> ")
			chatView.SetText("")
			go updateSessionList()
			return
		}
		// Forward other commands (like /status or /help) to the command handler
		handleCommandInput(resolved+" "+strings.Join(parts[1:], " "), h)
		return
	}

	sessionMu.RLock() // <--- RLock to get stream and rw
	sessionEntry, ok := sessions[activePeerID]
	sessionMu.RUnlock()

	if !ok {
		fmt.Fprintln(commandView, "[red]Error: No active session selected.[-]")
		return
	}

	rw := sessionEntry.RW // Use the stored ReadWriter

	// Send the message to the peer in a goroutine to avoid freezing the UI thread if the network buffer is full
	go func(msg string, target peer.ID) {
		if _, err := rw.WriteString(msg + "\n"); err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(commandView, "[red]Error sending message to %s: %s[-]\n", target, err)
			})
			return
		}
		rw.Flush()
	}(input, activePeerID)

	sessionMu.RLock()
	targetName := peerAliases[activePeerID]
	sessionMu.RUnlock()

	idx := registerPeer(activePeerID)
	if targetName == "" {
		targetName = fmt.Sprintf("$%d", idx)
	}

	msg := fmt.Sprintf("[blue]%s[-]: %s\n", tview.Escape(fmt.Sprintf("[me>%s]", targetName)), input)
	sessionMu.Lock()
	sessionBuffers[activePeerID] += msg
	sessionMu.Unlock()
	fmt.Fprint(chatView, msg)
}

func sendFile(target peer.ID, path string) {
	file, err := os.Open(path)
	if err != nil {
		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "[red]Cannot open file: %s[-]\n", err)
		})
		return
	}
	defer file.Close()

	fi, _ := file.Stat()
	fileName := filepath.Base(path)
	fileSize := fi.Size()

	app.QueueUpdateDraw(func() {
		fmt.Fprintf(commandView, "[yellow][File] Sending '%s' (%d bytes)...[-]\n", fileName, fileSize)
	})

	s, err := h.NewStream(ctx, target, fileProtocolID)
	if err != nil {
		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "[red]Failed to open file stream: %s[-]\n", err)
		})
		return
	}
	defer s.Close()

	// Send header: name\nsize\n
	header := fmt.Sprintf("%s\n%d\n", fileName, fileSize)
	s.Write([]byte(header))

	_, err = io.Copy(s, file)
	if err != nil {
		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "[red]Error during file transfer: %s[-]\n", err)
		})
	} else {
		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "[green][File] Transfer of '%s' complete.[-]\n", fileName)
		})
	}
}

// ****************************************************************************
// handleAuthInput()
// ****************************************************************************
func handleAuthInput(input string) {
	sessionMu.RLock()
	ch := authChan
	sessionMu.RUnlock()

	if ch == nil {
		return
	}

	line := strings.TrimSpace(input)
	signal := "N"
	if strings.ToLower(line) == "y" {
		signal = "Y"
	}

	select {
	case ch <- signal:
	default:
	}

	if signal == "N" {
		restoreUILabel()
	}
}

func restoreUILabel() {
	app.QueueUpdateDraw(func() {
		sessionMu.RLock()
		peerID := activePeerID
		sessionMu.RUnlock()

		idx := 0
		if peerID != "" {
			idx = registerPeer(peerID)
		}

		if currentStatus == StateInSession {
			inputField.SetLabel(fmt.Sprintf("[Chatting with $%d]: ", idx))
		} else {
			inputField.SetLabel("> ")
		}
	})
}

// ****************************************************************************
// runChatLoop()
// ****************************************************************************
func runChatLoop(remoteID peer.ID) {
	sessionMu.RLock()
	sessionEntry, ok := sessions[remoteID]
	sessionMu.RUnlock()
	if !ok {
		return // Session no longer exists (e.g., closed by /quit command)
	}
	s, rw := sessionEntry.Stream, sessionEntry.RW
	idx := registerPeer(remoteID)
	for {
		// Use the existing Reader to avoid losing data already in the buffer
		msg, err := rw.ReadString('\n')
		if err != nil {
			break
		}

		sessionMu.RLock()
		senderName := peerAliases[remoteID]
		sessionMu.RUnlock()
		if senderName == "" {
			senderName = fmt.Sprintf("$%d", idx)
		}

		msg = strings.TrimSpace(msg)
		formatted := fmt.Sprintf("[yellow]%s[-]: %s\n", tview.Escape(fmt.Sprintf("[%s>me]", senderName)), msg)

		sessionMu.Lock()
		sessionBuffers[remoteID] += formatted
		sessionMu.Unlock()

		app.QueueUpdateDraw(func() {
			if activePeerID == remoteID {
				fmt.Fprint(chatView, formatted)
			}
		})
	}

	sessionMu.Lock()
	delete(sessions, remoteID)
	sessionMu.Unlock()
	updateSessionList()

	sessionMu.RLock()
	isActive := activePeerID == remoteID
	sessionMu.RUnlock()

	if isActive {
		app.QueueUpdateDraw(func() {
			fmt.Fprintln(commandView, "[red][!] Connection lost.[-]")
			currentStatus = StateIdle
			inputField.SetLabel("> ")
		})
	}
	s.Close()
}

func updateSessionList() {
	if sessionListView == nil {
		return
	}

	// Snapshot the peers first to avoid holding peerMu while taking sessionMu (Deadlock Prevention)
	peerMu.Lock()
	peers := make([]peer.ID, len(discoveredPeers))
	copy(peers, discoveredPeers)
	peerMu.Unlock()

	sb := new(strings.Builder)
	sessionMu.RLock()
	currActive := activePeerID
	currStatus := currentStatus

	for i, id := range peers {
		if _, exists := sessions[id]; exists {
			displayName := id.String()
			if alias, ok := peerAliases[id]; ok && alias != "" {
				displayName = alias
			}

			indicator := "  "
			if id == currActive && currStatus == StateInSession {
				indicator = "[green]* [-]"
			}
			fmt.Fprintf(sb, "%s[$%d]> %s\n", indicator, i+1, displayName)
		}
	}
	sessionMu.RUnlock()

	newText := sb.String()
	app.QueueUpdateDraw(func() {
		sessionListView.SetText(newText)
	})
}

// discoverPeers handles the Rendezvous logic
func discoverPeers(ctx context.Context, h host.Host, idht *dht.IpfsDHT, rendezvous string) {
	routingDiscovery := drouting.NewRoutingDiscovery(idht)

	// Advertise our presence in this room
	util.Advertise(ctx, routingDiscovery, rendezvous)

	ticker := time.NewTicker(time.Second * 20)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peers, err := routingDiscovery.FindPeers(ctx, rendezvous)
			if err != nil {
				continue
			}
			for p := range peers {
				if p.ID == h.ID() {
					continue // Don't connect to ourselves
				}
				idx := registerPeer(p.ID)
				if h.Network().Connectedness(p.ID) != network.Connected {
					if err := h.Connect(ctx, p); err == nil {
						app.QueueUpdateDraw(func() {
							fmt.Fprintf(commandView, "[green][Global Discovery][-]: Found peer [$%d] %s in room '%s'\n", idx, p.ID, rendezvous)
						})
					}
				}
			}
		}
	}
}

// ****************************************************************************
// SERVER MODE FUNCTIONS
// ****************************************************************************

func runBootstrapServer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("[*] Starting Standalone Poop Bootstrap Node...")

	// 1. Load or Generate Identity
	priv, err := loadOrGenerateKey("bootstrap.key")
	if err != nil {
		fmt.Printf("[!] Identity error: %v\n", err)
		return
	}

	// 2. Initialize the Libp2p Host
	// Using port 40001 to bypass ISP restrictions on ports under 40000
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", defaultBootstrapPort),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", defaultBootstrapPort),
		),
		libp2p.NATPortMap(),
		libp2p.EnableRelay(),
	)
	if err != nil {
		fmt.Printf("[!] Failed to create host: %v\n", err)
		return
	}
	defer h.Close()

	// 3. Initialize DHT in Server Mode
	kdht, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
	if err != nil {
		fmt.Printf("[!] Failed to initialize DHT: %v\n", err)
		return
	}

	if err = kdht.Bootstrap(ctx); err != nil {
		fmt.Printf("[!] DHT Bootstrap error: %v\n", err)
		return
	}

	// 4. Output connectivity info
	fmt.Println("\n============================================================")
	fmt.Println("BOOTSTRAP NODE ONLINE")
	fmt.Printf("Peer ID: %s\n", h.ID())

	// Track addresses we've already printed to avoid duplicates
	seenAddrs := make(map[string]bool)
	printNewAddrs := func() {
		for _, addr := range h.Addrs() {
			fullAddr := fmt.Sprintf("%s/p2p/%s", addr, h.ID())
			if !seenAddrs[fullAddr] {
				fmt.Printf("[+] Detected Address: %s\n", fullAddr)
				seenAddrs[fullAddr] = true
			}
		}
	}

	printNewAddrs()
	fmt.Println("============================================================")
	fmt.Println("\nMonitoring for new network addresses (AutoNAT/Relay)...")
	fmt.Println("Press Ctrl+C to stop the server.")

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				printNewAddrs()
			}
		}
	}()

	// 5. Wait for termination signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n[*] Shutting down...")
}

func loadOrGenerateKey(path string) (crypto.PrivKey, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fmt.Printf("[+] No identity found. Generating new key: %s\n", path)
		priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		data, err := crypto.MarshalPrivateKey(priv)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			return nil, err
		}
		return priv, nil
	}

	fmt.Printf("[+] Loading existing identity from %s\n", path)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return crypto.UnmarshalPrivateKey(data)
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return !os.IsNotExist(err) && !info.IsDir()
}

func checkForUpdates() {
	// Extract the short hash from GitVersion (format: "MINOR-HASH")
	parts := strings.Split(GitVersion, "-")
	currentHash := parts[len(parts)-1]

	// Skip check for dev builds or if the version string is empty
	if currentHash == "dev" || currentHash == "no-git" || currentHash == "" {
		return
	}

	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("GET", "https://api.github.com/repos/jplozf/poop/commits/HEAD", nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "poop-p2p-client")
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var commit struct {
		Sha string `json:"sha"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&commit); err != nil {
		return
	}

	// If the remote SHA does not start with our local short hash, a new version exists
	if !strings.HasPrefix(commit.Sha, currentHash) {
		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "[yellow]A newer version is available on GitHub ! (Latest: %s)[-]\n", commit.Sha[:7])
			fmt.Fprintf(commandView, "[yellow]Please visit %s to download the latest version)[-]\n", appURL)
		})
	}
}

func generateRandomAlias() string {
	return fmt.Sprintf("Pooper-%04d", mrand.Intn(10000))
}

func saveAliasConfig(alias string) error {
	os.MkdirAll(filepath.Dir(configPath), 0755)
	cfg := struct {
		Alias string `json:"alias"`
	}{Alias: alias}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath, data, 0644)
}

/*
Standard Public Nodes:
/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvLcZunBNqv9U7Zkx6n6TVv4N497Xp9EWiZfWob
/ip4/104.236.179.241/tcp/4001/p2p/QmSoLP6zG1bsNqzqc8v9S7NmE6BNdnJa87u6pf8p8zKk5K
/ip4/128.199.219.111/tcp/4001/p2p/QmSoLSafvU76usqS8ELXWwLyBp7FLaycWvevP4cW7uWj6T
/ip4/104.236.76.40/tcp/4001/p2p/QmSoLMeWqB7YGVL2ox6V2Wv7VzYF6s9oV9mC2y2kYfU7pX
/ip4/178.62.158.247/tcp/4001/p2p/QmSoLer265NRztuWsZURrshBWo658FmAn9TFnfp93Y68t6
*/
