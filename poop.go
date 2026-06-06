package main

// ****************************************************************************
// IMPORTS
// ****************************************************************************
import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
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
	protocolID                 = "/poop/sync/1.0.0"
	StateIdle         AppState = iota
	StateAwaitingAuth          // We received a request, waiting for user to type 'y' or 'n'
	StateInSession             // We are actively talking to someone
)

// ****************************************************************************
// VARS
// ****************************************************************************
var (
	currentStatus = StateIdle
	pendingStream network.Stream
	historyFile   = filepath.Join(os.TempDir(), ".poop_history")
	ctx           = context.Background()
	h             host.Host
	kademliaDHT   *dht.IpfsDHT

	// UI Components
	app         *tview.Application
	commandView *tview.TextView
	chatView    *tview.TextView
	inputField  *tview.InputField
	history     []string
	historyIdx  = -1

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

// discoveryNotifee gets notified when we find a new peer via mDNS
type discoveryNotifee struct {
	h host.Host
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
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
	// 1. Initialize UI
	app = tview.NewApplication()

	commandView = tview.NewTextView().
		SetDynamicColors(true).
		SetRegions(true).
		SetWordWrap(true).
		SetChangedFunc(func() {
			commandView.ScrollToEnd()
			app.Draw()
		})
	commandView.SetBorder(true).SetTitle(" System & Commands ")

	chatView = tview.NewTextView().
		SetDynamicColors(true).
		SetWordWrap(true).
		SetChangedFunc(func() {
			chatView.ScrollToEnd()
			app.Draw()
		})
	chatView.SetBorder(true).SetTitle(" Chat Session ")

	inputField = tview.NewInputField().
		SetLabel("> ").
		SetFieldWidth(0)

	setupInputHandlers()

	// Layout: Top row (Command | Chat), Bottom row (Input)
	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(tview.NewFlex().
			AddItem(commandView, 0, 1, false).
			AddItem(chatView, 0, 1, false), 0, 1, false).
		AddItem(inputField, 1, 1, true)

	fmt.Fprintln(commandView, "[yellow]Welcome to Poop P2P.[-]")

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
		app.QueueUpdateDraw(func() {
			fmt.Fprintf(chatView, "[Peer]: %s", str)
		})
		s.Close()
	})

	h.SetStreamHandler("/poop/auth/1.0.0", func(s network.Stream) {
		if currentStatus != StateIdle {
			s.Write([]byte("REJECT_BUSY\n"))
			s.Close()
			return
		}

		pendingStream = s
		currentStatus = StateAwaitingAuth

		app.QueueUpdateDraw(func() {
			fmt.Fprintf(commandView, "[yellow][!!!] Incoming session from %s[-]\n", s.Conn().RemotePeer())
			inputField.SetLabel("Accept ? (y/n): ")
		})
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
		line := inputField.GetText()
		if len(line) == 0 {
			return
		}

		// Add to history if it's different from the last entry
		if len(history) == 0 || history[len(history)-1] != line {
			history = append(history, line)
		}
		historyIdx = -1

		inputField.SetText("")

		switch currentStatus {
		case StateAwaitingAuth:
			handleAuthInput(line)
		case StateInSession:
			handleSessionInput(line)
		case StateIdle:
			processedLine := line
			if strings.HasPrefix(line, "/") {
				processedLine = strings.TrimPrefix(line, "/")
			}
			handleCommandInput(processedLine, h)
		}
	})
}

// ****************************************************************************
// startSession()
// ****************************************************************************
func startSession(ctx context.Context, h host.Host, target string) {
	var info *peer.AddrInfo

	// 1. Try to parse as a full Multiaddr first (contains both ID and Network Addr)
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
			fmt.Fprintf(commandView, "[red]Input is not a valid Multiaddr or Peer ID: %s[-]\n", err)
			return
		}

		// Check the Peerstore for addresses associated with this ID.
		// These are populated automatically by mDNS and DHT discovery.
		knownAddrs := h.Peerstore().Addrs(id)
		if len(knownAddrs) == 0 {
			fmt.Fprintf(commandView, "[red]No known addresses for peer %s. Try using a full multiaddr.[-]\n", id)
			return
		}
		info = &peer.AddrInfo{ID: id, Addrs: knownAddrs}
	}

	// 3. Connect to the peer
	fmt.Fprintf(commandView, "Attempting to connect to %s...\n", info.ID)
	if err := h.Connect(ctx, *info); err != nil {
		fmt.Fprintf(commandView, "[red]Connection failed: %s[-]\n", err)
		return
	}

	// 4. Open the 'Poop' protocol stream
	s, err := h.NewStream(ctx, info.ID, "/poop/auth/1.0.0")
	if err != nil {
		fmt.Fprintf(commandView, "[red]Protocol error: %s[-]\n", err)
		return
	}

	// 5. Wait for the ACK/REJECT
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	fmt.Fprintln(commandView, "Waiting for peer to accept the session...")

	// We send a tiny 'knock' message
	rw.WriteString("SESSION_REQUEST\n")
	rw.Flush()

	reply, err := rw.ReadString('\n')
	if err != nil {
		fmt.Fprintln(commandView, "[red]Peer closed the connection.[-]")
		s.Close()
		return
	}

	if strings.TrimSpace(reply) == "ACK" {
		fmt.Fprintln(commandView, "[green]Success! Peer accepted the session.[-]")
		inputField.SetLabel("[SESSION]: ")
		pendingStream = s
		currentStatus = StateInSession
		startReadLoop(pendingStream)
	} else {
		fmt.Fprintln(commandView, "[red]Peer rejected the session request.[-]")
		s.Close()
	}
}

// ****************************************************************************
// handleCommandInput()
// ****************************************************************************
func handleCommandInput(input string, h host.Host) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return
	}

	command := parts[0]

	switch command {
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

	case "help":
		fmt.Fprintln(commandView, "Available commands: connect <addr>, room <name>, id, peers, status, exit, quit, bye, help")

	case "id":
		fmt.Fprintf(commandView, "[yellow]Share this address with your peer:[-]\n")
		addr := h.Addrs()[0] // Use the first available addr
		fmt.Fprintf(commandView, "[white]%s/p2p/%s[-]\n", addr, h.ID())

	case "exit", "quit", "bye":
		app.Stop()

	default:
		fmt.Fprintf(commandView, "[red]Unknown command: %s.[-] Type 'help' for info.\n", command)
	}
}

// ****************************************************************************
// handleSessionInput()
// ****************************************************************************
func handleSessionInput(input string) {
	// Check if the input is a command (starts with /)
	if strings.HasPrefix(input, "/") {
		if input == "/quit" {
			fmt.Fprintln(commandView, "Closing session...")
			pendingStream.Close()
			currentStatus = StateIdle
			inputField.SetLabel("> ")
			return
		}
		// Forward other commands (like /status or /help) to the command handler
		handleCommandInput(strings.TrimPrefix(input, "/"), h)
		return
	}

	fmt.Fprintf(chatView, "[blue][Me]: %s[-]\n", input)

	// We use the 'pendingStream' we saved earlier during the ACK
	rw := bufio.NewReadWriter(bufio.NewReader(pendingStream), bufio.NewWriter(pendingStream))

	// Send the message to the peer
	if _, err := rw.WriteString(input + "\n"); err != nil {
		fmt.Fprintf(commandView, "[red]Error sending message: %s[-]\n", err)
		currentStatus = StateIdle
		inputField.SetLabel("> ")
		return
	}
	rw.Flush()
}

// ****************************************************************************
// handleAuthInput()
// ****************************************************************************
func handleAuthInput(input string) {
	rw := bufio.NewReadWriter(bufio.NewReader(pendingStream), bufio.NewWriter(pendingStream))

	if strings.ToLower(input) == "y" {
		rw.WriteString("ACK\n")
		rw.Flush()
		currentStatus = StateInSession
		inputField.SetLabel("[SESSION]: ")
		fmt.Fprintln(commandView, "[green]--- Session Started ---[-]")
		startReadLoop(pendingStream)
	} else {
		rw.WriteString("REJECT\n")
		rw.Flush()
		pendingStream.Close()
		currentStatus = StateIdle
		inputField.SetLabel("> ")
		fmt.Fprintln(commandView, "Connection declined.")
	}
}

// ****************************************************************************
// startReadLoop()
// ****************************************************************************
func startReadLoop(s network.Stream) {
	go func() {
		scanner := bufio.NewScanner(s)
		for scanner.Scan() {
			msg := scanner.Text()
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(chatView, "[Peer]: %s\n", msg)
			})
		}
		if err := scanner.Err(); err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintln(commandView, "[red][!] Connection lost.[-]")
				currentStatus = StateIdle
				inputField.SetLabel("> ")
			})
		}
	}()
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
