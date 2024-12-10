package node

import (
	"distributed-chord/utils"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ChunkInfo struct {
	Key       int
	ChunkName string
}

func (n *Node) Chunker(fileName string, targetNodeIP string, startTime time.Time) []ChunkInfo {
	dataDir := "/local" // Change if needed
	var chunkSize int
	const TargetRetry = 10 * time.Second
	var chunks []ChunkInfo

	// checking if the file exists in the loacl file path of the docker container
	filePath := filepath.Join(dataDir, fileName)
	fileInfo, err := os.Stat(filePath)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("File %s does not exist in directory %s\n", fileName, dataDir)
		return nil
	} else if err != nil {
		fmt.Printf("Error checking file existence: %v\n", err)
		return nil
	}

	fileSize := fileInfo.Size()    // Convert to KB
	fileSizenew := fileSize / 1000 // Convert to MB

	// Calculate number of chunks using logarithmic scaling
	// log2(fileSize) rounded up, with a minimum of 1 and maximum of 20
	numChunks := int(math.Ceil(math.Log2(math.Max(float64(fileSizenew), 1))))
	// numChunks = max(1, min(numChunks, 20))

	// Dynamically calculate chunk size
	chunkSize = int(math.Ceil(float64(fileSize) / float64(numChunks)))
	fmt.Printf("Chunk size: %v, number of Chunks: %v", chunkSize, numChunks)
	// Open the source file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()

	ext := filepath.Ext(fileName)
	baseName := strings.TrimSuffix(fileName, ext)

	buffer := make([]byte, chunkSize)
	chunkNumber := 1

	// Single node failure - Simulate node failure before chunking (before sending chunk info)
	// fmt.Println("Waiting for 10 seconds before chunking. You can now kill the sender node.")
	// time.Sleep(10 * time.Second)

	for {
		bytesRead, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			fmt.Println("Error reading file:", err)
			return nil
		}
		if bytesRead == 0 {
			break
		}

		// Create the chunk file name by appending the chunk number at the end of the sanitized path without extension
		os.Setenv("TZ", "Asia/Singapore")
		timestamp := time.Now().In(time.Local).Format("02012006_150405")
		chunkFileName := fmt.Sprintf("%s-chunk-%d-%d-%s%s", baseName, chunkNumber, n.ID, timestamp, ext)
		chunkFilePath := filepath.Join(dataDir, chunkFileName)
		err = os.WriteFile(chunkFilePath, buffer[:bytesRead], 0644)
		if err != nil {
			fmt.Printf("Error writing chunk file %s: %v\n", chunkFileName, err)
			return nil
		}

		fmt.Printf("Chunk %d written: %s\n", chunkNumber, chunkFilePath)
		hashedKey := utils.Hash(chunkFileName)
		chunks = append(chunks, ChunkInfo{
			Key:       hashedKey,
			ChunkName: chunkFileName,
		})

		//Simulate Target Node failure during chunking

		//Force exit the target node after writing a few chunks (e.g., after 2 chunks)
		// if chunkNumber == 2 {
		// 	fmt.Printf("Triggering target node failure...\n")
		// 	_, err = CallRPCMethod(targetNodeIP, "Node.ForceExit", Message{})
		// 	if err != nil {
		// 		fmt.Printf("Failed to trigger target node failure: %v\n", err)
		// 	}
		// }

		fmt.Printf("Chunks: %v\n", chunks)
		chunkNumber++
	}

	fmt.Println("Sending the chunks to the receiver folder of the target node ...")
	err = n.send(chunks, targetNodeIP)
	if err != nil {
		fmt.Printf("Target Node is down,failed to send chunks to target node: %v\n", err)
		// Cleanup chunks since sending failed
		n.removeChunksRemotely(localFolder, chunks)
		n.removeChunksRemotely(dataFolder, chunks)
		return nil
	}

	// Send the chunk info to the target node for assembling
	elapsedTime := time.Since(startTime).Seconds()
	if elapsedTime >= 10 {
		fmt.Println("\nFile transfer took longer than expected. Please retry.")
		// Clean up chunks
		n.removeChunksRemotely(localFolder, chunks)
		n.removeChunksRemotely(dataFolder, chunks)
		return nil
	}

	message := Message{
		ID: n.ID,
		IP: n.IP,
		ChunkTransferParams: ChunkTransferRequest{
			Chunks: chunks,
		},
	}
	fmt.Printf("Sending chunk info to the target node at %s. Chunk info %v\n", targetNodeIP, chunks)

	// Uncomment this for target node sleeping before receiving chunk info
	fmt.Printf("Going to send to chunk location receiver\n")
	// fmt.Printf("Kill the target node in the 3 second duration.\n")
	// time.Sleep(3 * time.Second)

	retryInterval := 2 * time.Second
	retryStartTime := time.Now()
	var sendErr error

	for time.Since(retryStartTime) < TargetRetry {
		_, sendErr = CallRPCMethod(targetNodeIP, "Node.ChunkLocationReceiver", message)
		if sendErr == nil {
			// Successfully sent the chunk info
			break
		}
		fmt.Printf("Failed to send chunk info to target node: %v. Retrying in %v...\n", sendErr, retryInterval)
		time.Sleep(retryInterval)
	}

	if sendErr != nil {
		fmt.Printf("Failed to send chunk info to target node after %v: %v\n", TargetRetry, sendErr)
		n.removeChunksRemotely(localFolder, chunks)
		n.removeChunksRemotely(dataFolder, chunks)
		return nil
	}

	n.removeChunksRemotely(localFolder, chunks)

	return chunks
}

// ReceiveChunk handles receiving a chunk and saving it to the shared directory
func (n *Node) ReceiveChunk(request Message, reply *Message) error {
	destinationPath := filepath.Join("/shared", request.ChunkTransferParams.ChunkName)

	// Write the chunk data to the shared directory
	err := os.WriteFile(destinationPath, request.ChunkTransferParams.Data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write chunk to %s: %v", destinationPath, err)
	}

	*reply = Message{Type: "CHUNK_TRANSFER", ChunkTransferParams: request.ChunkTransferParams}
	return nil
}

func (n *Node) send(chunks []ChunkInfo, targetNodeIP string) error {
	for _, chunk := range chunks {
		var key = chunk.Key
		var chunkName = chunk.ChunkName

		message := Message{ID: key}
		var reply Message
		err := n.FindSuccessor(message, &reply)
		if err != nil {
			fmt.Printf("Failed to find successor: %v\n", err)
			continue
		}
		sendToNodeIP := reply.IP
		fmt.Printf("Sending chunk %s to node IP: %s\n", chunkName, sendToNodeIP)

		// Get the successor list of the node
		successorReply, err := CallRPCMethod(sendToNodeIP, "Node.GetSuccessorList", Message{})
		if err != nil {
			fmt.Printf("Failed to get successor list: %v\n", err)
			continue
		}
		successorList := successorReply.SuccessorList
		fmt.Printf("Successor list: %v\n", successorList)

		// Read the chunk data from the local directory
		chunkPath := filepath.Join("/local", chunkName)
		data, err := os.ReadFile(chunkPath)
		if err != nil {
			fmt.Printf("Failed to read chunk %s: %v\n", chunkName, err)
			continue
		}

		// Create the chunk transfer request
		request := Message{
			Type: "CHUNK_TRANSFER",
			ChunkTransferParams: ChunkTransferRequest{
				ChunkName: chunkName,
				Data:      data,
			},
		}

		_, err = CallRPCMethod(sendToNodeIP, "Node.ReceiveChunk", request)
		if err != nil {
			fmt.Printf("Failed to send chunk %s to node %s: %v\n", chunkName, sendToNodeIP, err)
			continue
		}

		fmt.Printf("Chunk %s sent successfully to node %s\n", chunkName, sendToNodeIP)

		for i := 0; i < len(successorList); i++ {
			request2 := Message{
				Type: "CHUNK_TRANSFER",
				ChunkTransferParams: ChunkTransferRequest{
					ChunkName: chunkName,
					Data:      data,
				},
			}

			successor := successorList[i]
			_, err = CallRPCMethod(successor.IP, "Node.ReceiveChunk", request2)
			fmt.Printf("Sending chunk to successor node with IP %v\n", successor.IP)
			if err != nil {
				fmt.Printf("Failed to send chunk %s to node %s: %v\n", chunkName, successor.IP, err)
				continue
			}
			fmt.Printf("Chunk %s sent successfully to node %s\n", chunkName, successor.IP)
		}

	}
	fmt.Printf("Chunk info sent successfully to node %s\n", targetNodeIP)
	return nil
}

// func (n *Node) GetSuccessorList(args *Message, reply *Message) error {
// 	successorIPs := []string{}
// 	for _, successor := range n.SuccessorList {
// 		successorIPs = append(successorIPs, successor.IP)
// 	}
// 	reply.SuccessorList = successorIPs
// 	return nil
// }

func (n *Node) ChunkLocationReceiver(message Message, reply *Message) error {

	// Fault Tolerance - Torget node is unreachabele/sleeping before the chunks array are sent (may or may not come back alive)
	// fmt.Printf("[NODE-%d] Simulating sleep. Ignoring requests for 12 seconds...Kill the current node\n", n.ID)
	// os.Exit(0)

	// Validate chunk information
	if message.ChunkTransferParams.Chunks == nil || len(message.ChunkTransferParams.Chunks) == 0 {
		return fmt.Errorf("no chunks to process")
	}

	//Simulate target node failure before assembly
	os.Exit(1)

	// Create a copy of the chunks to pass to the goroutine
	chunksCopy := make([]ChunkInfo, len(message.ChunkTransferParams.Chunks))
	copy(chunksCopy, message.ChunkTransferParams.Chunks)

	// Single node failure - Simulate node failure before chunking (before sending chunk info)
	// time.Sleep(10 * time.Second)

	done := make(chan error, 1)
	// Trigger assembler as a goroutine
	go func() {
		// Simulate a pause to allow demonstration of node disconnection
		// fmt.Println("Waiting for 30 seconds before assembly. You can now kill the sender node.")

		// Create a new message for the assembler
		assemblerMessage := Message{
			ID: message.ID,
			IP: message.IP,
			ChunkTransferParams: ChunkTransferRequest{
				Chunks: chunksCopy,
			},
		}

		var assemblerReply Message

		// Multiple Node Failures - Simulate node failure after chunking/before assembly
		// fmt.Printf("Pausing for 20 seconds before assembly. Crash other nodes now.\n")
		// time.Sleep(20 * time.Second)

		err := n.Assembler(assemblerMessage, &assemblerReply)
		done <- err
	}()

	select {
	case err := <-done:
		// If the target node during assembly using os.Exit(1)
		if err != nil {

			return err

		}

	// If the target node is down during assembly using time.sleep()
	case <-time.After(60 * time.Second):

		return fmt.Errorf("assembly timeout,target node is asleep.")

	}

	// Immediately return to allow sender to disconnect
	*reply = Message{
		Type: "CHUNK_LOCATIONS_RECEIVED",
	}
	return nil
}

func (n *Node) AssemblerComplete(message Message, reply *Message) error {
	green := "\033[32m" // ANSI code for red text
	reset := "\033[0m"  // ANSI code to reset color
	fmt.Printf("File Transfer has successfully completed.\n")
	fmt.Printf(green+"Time taken: %v\n"+reset, time.Since(n.StartReq))
	return nil
}
