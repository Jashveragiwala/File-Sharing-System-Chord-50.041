package node

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	localFolder    = "/local"
	dataFolder     = "/shared"   // Directory where the chunks are stored
	assembleFolder = "/assemble" // Directory where all the chunks retrieved from the nodes are stored
	outputFolder   = "/output"   // Directory where the assembled file is stored
)

// Assembler is a function that assembles the chunks of a file
func (n *Node) Assembler(message Message, reply *Message) error {
	n.Lock.Lock()
	n.AssemblerChunks = message.ChunkTransferParams.Chunks // Update the chunks list
	n.Lock.Unlock()

	// Single node failure - Simulate target node faliure during assembly
	// os.Exit(1)

	//Simulate node sleep during assembly, node will continue assembly process if it wakes up
	//time.Sleep(2 * time.Minute)

	if message.ChunkTransferParams.Chunks == nil || len(message.ChunkTransferParams.Chunks) == 0 {
		return fmt.Errorf("no chunks to assemble")
	}

	// Get the name of the first chunk to decipher the output file name and chunk template
	tempChunkFile := message.ChunkTransferParams.Chunks[0].ChunkName

	// Get the output file name from the chunk name
	outputFileName, err := getFileNames(tempChunkFile, message.ID)
	if err != nil {
		return fmt.Errorf(err.Error())
	}

	// Single node failure - Simulate node failure before/during assembly (after sending chunk info)
	fmt.Printf("Pausing for 10 seconds before assembly. You can stop the sender now to simulate failure.\n")
	time.Sleep(10 * time.Second)

	// Killing for experiment simulation
	// fmt.Printf("Pausing for 15 seconds before assembly. You can stop the node now to simulate failure.\n")
	// time.Sleep(15 * time.Second)

	// Multiple Node Failures - Simulate node failure after during assembly
	// fmt.Printf("Pausing for 20 seconds during assembly. Crash other nodes now.\n")
	// time.Sleep(20 * time.Second)

	err = n.getAllChunks(message.ChunkTransferParams.Chunks)
	if err != nil {
		fmt.Printf("Error collecting chunks: %v\n", err)
		return err
	}

	err = assembleChunks(outputFileName, message.ChunkTransferParams.Chunks)
	if err != nil {
		fmt.Printf("Error assembling chunks: %v\n", err)
		fmt.Printf("Aborting assembling...\n")
		return err
	}

	fmt.Printf("File %s assembled successfully\n", outputFileName)

	// Clean up the assemble folder
	n.removeChunksRemotely(assembleFolder, message.ChunkTransferParams.Chunks)
	n.removeChunksRemotely(dataFolder, message.ChunkTransferParams.Chunks)

	_, err = CallRPCMethod(message.IP, "Node.AssemblerComplete", Message{})
	if err != nil {
		// fmt.Printf("Error notifying sender of assembly completion: %v\n", err)
	}
	return nil
}

// Gets all the chunks from the nodes and compiles them into the /assemble folder.
func (n *Node) getAllChunks(chunkInfo []ChunkInfo) error {
	// Create the assemble folder if it doesn't exist
	if err := os.MkdirAll(assembleFolder, 0755); err != nil {
		return fmt.Errorf("error creating assemble folder: %v", err)
	}

	for _, chunk := range chunkInfo {
		var reply Message
		message := Message{
			ID: chunk.Key,
			ChunkTransferParams: ChunkTransferRequest{
				ChunkName: chunk.ChunkName,
			},
		}

		// Incase the node fails during assembly, we have upto 3 retries to handle it(can be changed)
		// time.Sleep(5 * time.Second)
		maxRetries := 3
		retries := 0
		chunkFound := false
		var chunkData []byte

		// Start of the retry loop
		for retries < maxRetries {
			// Find the successor of the chunk key
			n.FindSuccessor(message, &reply)
			targetNode := Pointer{ID: reply.ID, IP: reply.IP}

			// Introduce a sleep to simulate delay
			// fmt.Printf("Simulating delay before contacting target node %d (%s). You can stop the node now to simulate failure.\n", targetNode.ID, targetNode.IP)
			// time.Sleep(10 * time.Second)

			// Attempt to get the successor list from the target node
			successorReply, err := CallRPCMethod(targetNode.IP, "Node.GetSuccessorList", Message{})
			if err != nil {
				fmt.Printf("Failed to get successor list from node %d: %v\n", targetNode.ID, err)
				// Node might have failed; retry FindSuccessor
				retries++
				fmt.Printf("Retrying FindSuccessor for chunk %s (attempt %d of %d)\n", chunk.ChunkName, retries, maxRetries)
				continue // Retry from the beginning of the loop
			}

			// Initialize the list of nodes to try, starting with the target node
			nodesToTry := []Pointer{targetNode}
			// Append the successors to the nodesToTry list
			nodesToTry = append(nodesToTry, successorReply.SuccessorList...)

			// Iterate over the nodes to try
			for _, node := range nodesToTry {
				// Attempt to get the chunk from the node
				reply, err := CallRPCMethod(node.IP, "Node.SendChunk", message)
				if err != nil {
					fmt.Printf("Error receiving chunk %s from node %d: %v\n", chunk.ChunkName, node.ID, err)
					continue // Try the next node
				}

				// Check if the chunk data is present
				if reply.ChunkTransferParams.Data == nil || len(reply.ChunkTransferParams.Data) == 0 {
					fmt.Printf("Node %d does not have the chunk %s\n", node.ID, chunk.ChunkName)
					continue // Try the next node
				}

				// Chunk has been found
				chunkData = reply.ChunkTransferParams.Data
				chunkFound = true
				fmt.Printf("Chunk %s successfully retrieved from node %d\n", chunk.ChunkName, node.ID)
				break
			}

			if chunkFound {
				break // Exit the retry loop
			} else {
				// If we haven't found the chunk, increment retries and attempt FindSuccessor again
				retries++
				fmt.Printf("Chunk %s not found, retrying FindSuccessor (attempt %d of %d)\n", chunk.ChunkName, retries, maxRetries)
			}
		}

		if !chunkFound {
			return fmt.Errorf("failed to retrieve chunk %s from any node after %d attempts", chunk.ChunkName, maxRetries)
		}

		// Save the chunk data in the assemble directory
		destinationPath := filepath.Join(assembleFolder, chunk.ChunkName)
		err := os.WriteFile(destinationPath, chunkData, 0644)
		if err != nil {
			return fmt.Errorf("error writing chunk %s to %s: %v", chunk.ChunkName, destinationPath, err)
		}
	}
	return nil
}

// Function to assemble all the chunks from the assemble folder
func assembleChunks(outputFileName string, chunks []ChunkInfo) error {

	// Making the output file
	if err := os.MkdirAll(outputFolder, 0755); err != nil {
		return fmt.Errorf("error creating output folder: %v", err)
	}

	outputFilePath := filepath.Join(outputFolder, outputFileName)
	outFile, err := os.Create(outputFilePath)

	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer outFile.Close()

	for i, chunk := range chunks {
		// filename-chunk
		content, err := ioutil.ReadFile(filepath.Join(assembleFolder, chunk.ChunkName))
		if err != nil {
			return fmt.Errorf("error reading chunk %s-chunk%d.txt: %v", chunk.ChunkName, int(i+1), err)
		}

		_, err = outFile.Write(content)
		if err != nil {
			return fmt.Errorf("error writing chunk %s-chunk%d.txt to output file: %v", chunk.ChunkName, int(i+1), err)
		}
	}

	return nil
}

func getFileNames(chunkName string, senderID int) (string, error) {
	for i, v := range chunkName {
		if v == '-' && chunkName[i+1:i+6] == "chunk" {
			return chunkName[:i] + "_from_" + strconv.Itoa(senderID) + filepath.Ext(chunkName), nil
		}
	}
	return "", fmt.Errorf("error getting output file name")
}

// SendChunk handles sending a chunk to a requesting node
func (n *Node) SendChunk(request Message, reply *Message) error {
	sourcePath := filepath.Join(dataFolder, request.ChunkTransferParams.ChunkName)

	// Read the chunk data from the shared directory
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to read chunk from %s: %v", sourcePath, err)
	}

	// Send the chunk data as the reply
	*reply = Message{ChunkTransferParams: ChunkTransferRequest{
		Data: data,
	}}
	return nil
}
