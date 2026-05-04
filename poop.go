package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

const protocolID = "/p2p/exchange/1.0.0"

func main() {
	ctx := context.Background()

	// 1. Create the Libp2p Host
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
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

	// 3. Print local info so others can connect to us
	fmt.Println("Your Peer ID:", h.ID())
	for _, addr := range h.Addrs() {
		fmt.Printf("Connect to me at: %s/p2p/%s\n", addr, h.ID())
	}

	// 4. Interactive CLI loop
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("\nEnter peer address to send a message (or 'exit'): \n> ")
		if !scanner.Scan() {
			break
		}
		input := scanner.Text()

		if input == "exit" {
			break
		}

		// If the input looks like a multiaddress, try to connect
		err := sendMessage(ctx, h, input, "Hello from the other side!\n")
		if err != nil {
			fmt.Printf("Error: %s\n", err)
		}
	}
}

func sendMessage(ctx context.Context, h host.Host, targetAddr string, msg string) error {
	// Parse the multiaddr
	maddr, err := multiaddr.NewMultiaddr(targetAddr)
	if err != nil {
		return err
	}

	// Extract peer info (ID and address) from the multiaddr
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return err
	}

	// Connect to the remote peer
	if err := h.Connect(ctx, *info); err != nil {
		return err
	}

	// Open a new stream using our specific protocol ID
	s, err := h.NewStream(ctx, info.ID, protocolID)
	if err != nil {
		return err
	}

	// Write the message
	_, err = s.Write([]byte(msg))
	s.Close()
	fmt.Println("Message sent successfully!")
	return err
}
