package node

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Constants defining folder paths
const (
	localFolder    = "/local"
	dataFolder     = "/shared"   // Directory where the chunks are stored
	assembleFolder = "/assemble" // Directory where all the chunks retrieved from the nodes are stored
	outputFolder   = "/output"   // Directory where the assembled file is stored
)

// Assembler is a function that assembles the chunks of a file
func (n *Node) Assembler(message Message, reply *Message) error {
	// Lock the node to safely update the AssemblerChunks
	n.Lock.Lock()
	n.AssemblerChunks = message.ChunkTransferParams.Chunks // Update the chunks list
	n.Lock.Unlock()

	// Validate chunk information
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

	// Start timing the assembly process
	startTime := time.Now()

	// Retrieve all chunks with majority voting
	err = n.getAllChunks(message.ChunkTransferParams.Chunks)
	if err != nil {
		fmt.Printf("Error collecting chunks: %v\n", err)
		return err
	}

	// Assemble the chunks into the final file
	err = assembleChunks(outputFileName, message.ChunkTransferParams.Chunks)
	if err != nil {
		fmt.Printf("Error assembling chunks: %v\n", err)
		fmt.Printf("Aborting assembling...\n")
		return err
	}

	fmt.Printf("File %s assembled successfully\n", outputFileName)

	// Clean up the assemble and shared folders
	err = n.removeChunksRemotely(assembleFolder, message.ChunkTransferParams.Chunks)
	if err != nil {
		fmt.Printf("Error removing chunks from assemble folder: %v\n", err)
	}

	err = n.removeChunksRemotely(dataFolder, message.ChunkTransferParams.Chunks)
	if err != nil {
		fmt.Printf("Error removing chunks from shared folder: %v\n", err)
	}

	// Notify the sender of assembly completion with the time taken
	assemblyTime := time.Since(startTime)
	assemblyCompleteMsg := Message{
		Type: "ASSEMBLER_COMPLETE",
		// Since 'Content' is not defined in Message struct, we can utilize 'ChunkTransferParams.Data' to send the message.
		ChunkTransferParams: ChunkTransferRequest{
			Data: []byte(fmt.Sprintf("Assembly completed in %v", assemblyTime)),
		},
	}

	// CallRPCMethodWithTimeout is assumed to have the following signature:
	// func CallRPCMethodWithTimeout(ip string, method string, message Message, reply *Message, timeout time.Duration) error
	err = CallRPCMethodWithTimeout(message.IP, "Node.AssemblerComplete", assemblyCompleteMsg, reply, 5*time.Second)
	if err != nil {
		fmt.Printf("Error notifying sender of assembly completion: %v\n", err)
	}
	return nil
}

// / getAllChunks retrieves all replicas for each chunk and performs majority voting
func (n *Node) getAllChunks(chunkInfo []ChunkInfo) error {
	// Create the assemble folder if it doesn't exist
	if err := os.MkdirAll(assembleFolder, 0755); err != nil {
		return fmt.Errorf("error creating assemble folder: %v", err)
	}

	for _, chunk := range chunkInfo {
		message := Message{
			ID: chunk.Key,
			ChunkTransferParams: ChunkTransferRequest{
				ChunkName: chunk.ChunkName,
			},
		}

		// Find the primary node responsible for the chunk by calling FindSuccessor on the current node
		primaryReply, err := CallRPCMethod(n.IP, "Node.FindSuccessor", message)
		if err != nil {
			fmt.Printf("Error finding successor for chunk %s: %v\n", chunk.ChunkName, err)
			return err
		}

		// Initialize primaryNode with the reply
		primaryNode := Pointer{ID: primaryReply.ID, IP: primaryReply.IP}

		// Retrieve the primary node's successor list
		successorReply, err := CallRPCMethod(primaryNode.IP, "Node.GetSuccessorList", Message{})
		if err != nil {
			fmt.Printf("Failed to get successor list from primary node %s: %v\n", primaryNode.IP, err)
			return err
		}

		// Assuming SuccessorList is a slice of Pointer in the Message struct
		successorList := successorReply.SuccessorList

		// Collect all replicas: primary node + successors
		replicas := append([]Pointer{primaryNode}, successorList...)

		// Debug: Log the replicas being queried
		fmt.Printf("[getAllChunks] Replicas for chunk %s: %v\n", chunk.ChunkName, replicas)

		// Maps to store chunk hashes and their occurrence counts
		hashCount := make(map[string]int)
		// Map to store hash to actual data
		hashData := make(map[string][]byte)
		totalReplicas := 0

		for _, replica := range replicas {
			// Attempt to retrieve the chunk from the replica
			chunkData, err := n.retrieveChunk(replica.IP, chunk.ChunkName)
			if err != nil {
				fmt.Printf("Error retrieving chunk %s from Node-%d (%s): %v\n", chunk.ChunkName, replica.ID, replica.IP, err)
				continue
			}

			// Compute SHA-256 hash of the chunk data
			hashBytes := sha256.Sum256(chunkData)
			hash := fmt.Sprintf("%x", hashBytes)
			hashCount[hash]++
			// Store the data if not already stored
			if _, exists := hashData[hash]; !exists {
				hashData[hash] = chunkData
			}
			totalReplicas++
		}

		if totalReplicas == 0 {
			return fmt.Errorf("no replicas available for chunk %s", chunk.ChunkName)
		}

		// Determine the majority hash
		majorityHash, count := "", 0
		for hash, c := range hashCount {
			if c > count {
				majorityHash = hash
				count = c
			}
		}

		// Check if majority is achieved
		if count > totalReplicas/2 {
			// Retrieve the data corresponding to the majority hash
			majorityContent := hashData[majorityHash]
			destinationPath := filepath.Join(assembleFolder, chunk.ChunkName)
			err := os.WriteFile(destinationPath, majorityContent, 0644)
			if err != nil {
				return fmt.Errorf("error writing chunk %s to %s: %v", chunk.ChunkName, destinationPath, err)
			}
			fmt.Printf("Chunk %s assembled successfully with majority content from %d replicas.\n", chunk.ChunkName, count)
		} else {
			// Handle lack of majority
			fmt.Printf("No majority for chunk %s. Received %d different contents.\n", chunk.ChunkName, len(hashCount))
			// Optionally, choose the first available content or mark the chunk as corrupted
			var fallbackContent []byte
			for _, replica := range replicas {
				chunkData, err := n.retrieveChunk(replica.IP, chunk.ChunkName)
				if err != nil {
					continue
				}
				fallbackContent = chunkData
				break
			}
			if fallbackContent == nil {
				return fmt.Errorf("unable to retrieve any content for chunk %s", chunk.ChunkName)
			}
			destinationPath := filepath.Join(assembleFolder, chunk.ChunkName)
			err := os.WriteFile(destinationPath, fallbackContent, 0644)
			if err != nil {
				return fmt.Errorf("error writing chunk %s to %s: %v", chunk.ChunkName, destinationPath, err)
			}
			fmt.Printf("Chunk %s assembled with fallback content from the first available replica.\n", chunk.ChunkName)
		}
	}
	return nil
}

// assembleChunks concatenates all the chunks into the final output file
func assembleChunks(outputFileName string, chunks []ChunkInfo) error {

	// Creating the output folder if it doesn't exist
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
		// Read the chunk data from the assemble directory
		content, err := ioutil.ReadFile(filepath.Join(assembleFolder, chunk.ChunkName))
		if err != nil {
			return fmt.Errorf("error reading chunk %s-chunk%d.txt: %v", chunk.ChunkName, int(i+1), err)
		}

		// Write the chunk data to the output file
		_, err = outFile.Write(content)
		if err != nil {
			return fmt.Errorf("error writing chunk %s-chunk%d.txt to output file: %v", chunk.ChunkName, int(i+1), err)
		}
	}

	return nil
}

// getFileNames extracts the original file name from the chunk name
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
