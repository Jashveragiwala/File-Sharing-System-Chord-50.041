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

	err = CallRPCMethodWithTimeout(message.IP, "Node.AssemblerComplete", Message{}, reply, 5*time.Second)
	if err != nil {
		fmt.Printf("Error notifying sender of assembly completion: %v\n", err)
	}
	return nil
}

// / Gets all the chunks from the nodes and compiles them into the /assemble folder with majority voting.
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

		// Find the primary node responsible for the chunk
		err := n.FindSuccessor(message, &reply)
		if err != nil {
			fmt.Printf("Error finding successor for chunk %s: %v\n", chunk.ChunkName, err)
			return err
		}

		primaryNode := Pointer{ID: reply.ID, IP: reply.IP}
		successorList := n.SuccessorList

		// Collect all replicas: primary node + successors
		replicas := append([]Pointer{primaryNode}, successorList...)

		// Map to store chunk hashes and their occurrence counts
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
			hash := fmt.Sprintf("%x", sha256.Sum256(chunkData))
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
