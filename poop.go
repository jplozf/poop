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
	"strings"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/peterh/liner"
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
	err           error
)

// ****************************************************************************
// main()
// ****************************************************************************
func main() {
	line := liner.NewLiner()
	defer line.Close()

	// 1. Configure liner
	line.SetCtrlCAborts(true)

	// 2. Load previous history from a file
	if f, err := os.Open(historyFile); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	fmt.Println("Welcome to Poop P2P.")
	// 1. Create the Libp2p Host
	h, err = libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		panic(err)
	}
	defer h.Close()

	// 2. Setup the Listener (Receiver) logic
	h.SetStreamHandler(protocolID, func(s network.Stream) {
		fmt.Printf("\n[Incoming] New stream from %s\n", s.Conn().RemotePeer())
		rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))

		str, _ := rw.ReadString('\n')
		fmt.Printf("Received Message: %s", str)
		s.Close()
		fmt.Print("> ") // Reset the CLI prompt
	})

	h.SetStreamHandler("/poop/auth/1.0.0", func(s network.Stream) {
		if currentStatus != StateIdle {
			s.Write([]byte("REJECT_BUSY\n"))
			s.Close()
			return
		}

		pendingStream = s
		currentStatus = StateAwaitingAuth

		// Alert the user without breaking the line they are currently typing
		fmt.Printf("\n\n[!!!] Incoming session from %s", s.Conn().RemotePeer())
		fmt.Printf("\nAccept ? (y/n): ")
	})

	// 3. Print local info so others can connect to us
	fmt.Println("Your Peer ID :", h.ID())
	for _, addr := range h.Addrs() {
		fmt.Printf("Connect to me at: %s/p2p/%s\n", addr, h.ID())
	}

	for {
		prompt := "> "
		switch currentStatus {
		case StateAwaitingAuth:
			prompt = "Accept ? (y/n): "
		case StateInSession:
			prompt = "[SESSION]: "
		}

		input, err := line.Prompt(prompt)
		if err != nil {
			break
		}
		input = strings.TrimSpace(input)
		line.AppendHistory(input)

		switch currentStatus {
		case StateAwaitingAuth:
			handleAuthInput(input)
		case StateInSession:
			handleSessionInput(input)
		case StateIdle:
			handleCommandInput(input, h) // Your normal "connect <addr>" logic
		}
	}

	// 6. Save history to file before exiting
	if f, err := os.Create(historyFile); err == nil {
		line.WriteHistory(f)
		f.Close()
	}
}

// ****************************************************************************
// startSession()
// ****************************************************************************
func startSession(ctx context.Context, h host.Host, targetAddr string) {
	// 1. Parse the address
	maddr, err := multiaddr.NewMultiaddr(targetAddr)
	if err != nil {
		fmt.Printf("Invalid address: %s\n", err)
		return
	}

	// 2. Extract Peer ID and Address info
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		fmt.Printf("Could not get peer info: %s\n", err)
		return
	}

	// 3. Connect to the peer
	fmt.Printf("Attempting to connect to %s...\n", info.ID)
	if err := h.Connect(ctx, *info); err != nil {
		fmt.Printf("Connection failed: %s\n", err)
		return
	}

	// 4. Open the 'Poop' protocol stream
	s, err := h.NewStream(ctx, info.ID, "/poop/auth/1.0.0")
	if err != nil {
		fmt.Printf("Protocol error: %s\n", err)
		return
	}

	// 5. Wait for the ACK/REJECT
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	fmt.Println("Waiting for peer to accept the session...")

	// We send a tiny 'knock' message
	rw.WriteString("SESSION_REQUEST\n")
	rw.Flush()

	reply, err := rw.ReadString('\n')
	if err != nil {
		fmt.Println("Peer closed the connection.")
		s.Close()
		return
	}

	if strings.TrimSpace(reply) == "ACK" {
		fmt.Println("Success! Peer accepted the session.")
		// IMPORTANT: Set global state so handleSessionInput takes over
		pendingStream = s
		currentStatus = StateInSession
		startReadLoop(pendingStream)
	} else {
		fmt.Println("Peer rejected the session request.")
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
			fmt.Println("Usage: connect <multiaddr>")
			return
		}
		targetAddr := parts[1]
		// This is the function we wrote earlier that calls h.NewStream
		startSession(context.Background(), h, targetAddr)

	case "help":
		fmt.Println("Available commands: connect <addr>, exit, help")

	case "exit":
		fmt.Println("Goodbye!")
		os.Exit(0)

	default:
		fmt.Printf("Unknown command: %s. Type 'help' for info.\n", command)
	}
}

// ****************************************************************************
// handleSessionInput()
// ****************************************************************************
func handleSessionInput(input string) {
	if input == "/quit" {
		fmt.Println("Closing session...")
		pendingStream.Close()
		currentStatus = StateIdle
		return
	}

	// We use the 'pendingStream' we saved earlier during the ACK
	rw := bufio.NewReadWriter(bufio.NewReader(pendingStream), bufio.NewWriter(pendingStream))

	// Send the message to the peer
	_, err := rw.WriteString(input + "\n")
	if err != nil {
		fmt.Println("Error sending message:", err)
		currentStatus = StateIdle
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
		fmt.Println("--- Session Started ---")
		startReadLoop(pendingStream)
	} else {
		rw.WriteString("REJECT\n")
		rw.Flush()
		pendingStream.Close()
		currentStatus = StateIdle
		fmt.Println("Connection declined.")
	}
}

// ****************************************************************************
// startReadLoop()
// ****************************************************************************
func startReadLoop(s network.Stream) {
	go func() {
		scanner := bufio.NewScanner(s)
		for scanner.Scan() {
			// Clear the current line (the prompt), print the message, and restore the prompt
			// This prevents the incoming message from getting mixed with your typing
			fmt.Printf("\r\x1b[K[Peer]: %s\n> ", scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("\n[!] Connection lost.")
			currentStatus = StateIdle
		}
	}()
}
