package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

/*

This is a simple standalone bootstrap server for the Poop Protocol. It uses libp2p to create a peer-to-peer node that listens on a fixed port and participates in the Kademlia DHT in server mode.

Key Features:
- Generates or loads a persistent identity key pair.
- Listens on a fixed port (4001) for both TCP and QUIC connections.
- Initializes a Kademlia DHT in server mode to provide routing information to clients.
- Outputs its Peer ID and multiaddresses for clients to connect to.
- Gracefully handles shutdown on interrupt signals.

This server is designed to be simple and reliable, serving as a stable anchor point for clients in the Poop Protocol network.
This server is intended to be run on a public server with a static IP or domain name, and should be reachable by clients to help them discover each other and bootstrap their connections.
This server could be run if we want to have a public bootstrap node without dependency on a third-party service like IPFS's public bootstrap nodes. It can be run on any machine with Go installed, and it will create a persistent identity key in the current directory for consistent peer identity across restarts.

*/

// ****************************************************************************
// TYPES
// ****************************************************************************
// discoveryNotifee gets notified when we find a new peer via mDNS
type discoveryNotifee struct {
	h host.Host
}

// ****************************************************************************
// CONSTS
// ****************************************************************************

const (
	// Port 4001 is the standard default for libp2p/IPFS nodes, but could be any open port. Using a fixed port makes it easier for clients to connect.
	listenPort = 4001
	keyFile    = "bootstrap.key"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("[*] Starting Standalone Poop Bootstrap Node...")

	// 1. Load or Generate Identity
	priv, err := loadOrGenerateKey(keyFile)
	if err != nil {
		fmt.Printf("[!] Identity error: %v\n", err)
		return
	}

	// 2. Initialize the Libp2p Host
	// We use a fixed port so the address remains consistent for clients.
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", listenPort),
		),
		// Enable basic features required for a reliable backbone node
		libp2p.NATPortMap(),
		libp2p.EnableRelay(),
	)
	if err != nil {
		fmt.Printf("[!] Failed to create host: %v\n", err)
		return
	}
	defer h.Close()

	// 3. Initialize DHT in Server Mode
	// ModeServer means this node will store and provide routing records to others.
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
	fmt.Println("Addresses to use in your client:")
	for _, addr := range h.Addrs() {
		fmt.Printf("  %s/p2p/%s\n", addr, h.ID())
	}
	fmt.Println("============================================================")
	fmt.Println("\nPress Ctrl+C to stop the server.")

	// 5. Wait for termination signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n[*] Shutting down...")
}

func loadOrGenerateKey(path string) (crypto.PrivKey, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Generate new key
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

	// Load existing key
	fmt.Printf("[+] Loading existing identity from %s\n", path)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return crypto.UnmarshalPrivateKey(data)
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	// The bootstrap node doesn't need to do anything specific when finding peers,
	// its job is just to be reachable so others can find IT.
}
