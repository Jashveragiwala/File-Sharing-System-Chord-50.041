// main.go

package main

import (
	"distributed-chord/node"
	"flag"
	"fmt"
	"log"
	"os"
)

// "distributed-chord/node"

func main() {
	// fca.Chunker()
	//fca.Assembler(fca.ChunkInfo{[]string{"Node2", "Node4", "Node1"}, "C__Users_saran_Documents_GitHub_File-Sharing-System-Chord-50.041_Data_Node1_file"})
	// Parse command-line flags
	ip := flag.String("ip", "127.0.0.1", "IP address of the node")
	port := flag.Int("port", 8000, "Port number of the node")
	joinAddr := flag.String("join", "", "Address of an existing node to join (format: ip:port)")
	m := flag.Int("m", 5, "Key space size in bits")

	flag.Parse()

	// // Create a new node instance
	n := node.NewNode(*ip, *port, *m)

	// Start the gRPC server
	err := n.StartGRPCServer()
	if err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}

	// Join the network or create a new one
	if *joinAddr != "" {
		fmt.Printf("Node %x joining the network via %s...\n", n.ID, *joinAddr)
		err = n.JoinNetwork(*joinAddr)
		if err != nil {
			log.Fatalf("Failed to join network: %v", err)
		}
	} else {
		fmt.Printf("Node %x creating a new network...\n", n.ID)
		n.InitializeFingerTable()
	}

	// // Start periodic tasks (finger table updates and stabilization)
	n.StartPeriodicTasks()

	go func() {
		for {
			var command, key, value string
			fmt.Println("Enter command (put/get/exit):")
			fmt.Scanln(&command)
			switch command {
			case "put":
				fmt.Println("Enter key:")
				fmt.Scanln(&key)
				fmt.Println("Enter value:")
				fmt.Scanln(&value)
				err := n.PutKey(key, value)
				if err != nil {
					fmt.Printf("Error putting key: %v\n", err)
				} else {
					fmt.Println("Key stored successfully.")
				}
			case "get":
				fmt.Println("Enter key:")
				fmt.Scanln(&key)
				value, err := n.GetKey(key)
				if err != nil {
					fmt.Printf("Error getting key: %v\n", err)
				} else {
					fmt.Printf("Retrieved value: %s\n", value)
				}
			case "exit":
				fmt.Println("Exiting...")
				os.Exit(0)
			default:
				fmt.Println("Unknown command.")
			}
		}
	}()

	// Keep the main function running
	select {}
}
