package node

import (
	"distributed-chord/utils"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type Pointer struct {
	ID int    // Node ID
	IP string // Node IP address with the port
}

type Node struct {
	ID              int
	IP              string
	Successor       Pointer
	Predecessor     Pointer
	FingerTable     []Pointer
	SuccessorList   []Pointer
	StartReq        time.Time
	Lock            sync.Mutex
	AssemblerChunks []ChunkInfo // Field to store the assembler chunks
}

type NodeInfo struct {
	ID        int
	IP        string
	Successor Pointer
}

const (
	timeInterval = 5         // Time interval for stabilization and fixing fingers
	r            = 3         // Number of successors to keep in the successor list
	retries      = 3         // Number of retries for file transfer
	CONFIRM      = "CONFIRM" // Confirm file transfer
	REJECT       = "REJECT"  // Deny file transfer
)

var IsSleeping atomic.Bool

// Starting the RPC server for the nodes
func (n *Node) StartRPCServer() {
	IsSleeping.Store(false) // Initially no partition
	rpc.Register(n)
	listener, err := net.Listen("tcp", n.IP)
	if err != nil {
		fmt.Printf("[NODE-%d] Error starting RPC server: %v\n", n.ID, err)
		return
	}
	defer listener.Close()
	fmt.Printf("[NODE-%d] Listening on %s\n", n.ID, n.IP)

	for {
		conn, err := listener.Accept()
		if IsSleeping.Load() {
			fmt.Printf("[NODE-%d] Network partition detected. Waiting for recovery...\n", n.ID)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		if err != nil {
			fmt.Printf("[NODE-%d] accept error: %s\n", n.ID, err)
			return
		}
		go rpc.ServeConn(conn)
	}

}

func (n *Node) RequestFileTransfer(targetNodeID int, fileName string) error {
	var reply Message
	var err error
	message := Message{ID: targetNodeID}

	for i := 0; i < retries; i++ {
		err := n.FindSuccessor(message, &reply)
		if err != nil {
			return fmt.Errorf("failed to find successor: %v", err)
		}
		if reply.ID != targetNodeID {
			fmt.Printf("Node %d not found. Retrying to find successor (attempt %d of %d)\n", targetNodeID, i+1, retries)
			time.Sleep(3 * time.Second) // retry after 3 seconds
		} else {
			break
		}
	}

	// time.Sleep(5 * time.Second) // Sleep for checking if the find successor detects the target node as alive but is actually sleeping.
	targetNodeIP := reply.IP
	fmt.Printf("Target node IP: %s\n", targetNodeIP)

	if n.IP == targetNodeIP {
		fmt.Println("Cannot send file to the same node.")
		return nil
	}

	request := Message{
		IP:       n.IP,
		FileName: fileName,
	}

	var response *Message
	var success bool
	for i := 0; i < retries; i++ {
		success = false
		response, err = CallRPCMethod(targetNodeIP, "Node.ConfirmFileTransfer", request)
		if err != nil {
			// target node fail before chunking
			fmt.Printf("[NODE-%d] Error confirming file transfer.\n", n.ID)
			fmt.Printf("[NODE-%d] Retrying file transfer confirmation to node %d (attempt %d of %d)\n", n.ID, targetNodeID, i+1, retries)
			time.Sleep(3 * time.Second) // retry after 3 seconds
		} else {
			success = true
			break
		}
	}

	if !success {
		return fmt.Errorf("failed to confirm file transfer after %d attempts", retries)
	}

	if response.Type == CONFIRM {
		fmt.Println("\nTarget accepted the file transfer. Initiating transfer...")
		startTime := time.Now()
		n.StartReq = startTime
		chunks := n.Chunker(fileName, targetNodeIP, startTime)
		if len(chunks) > 0 {
			//i changed this to chunk transfer, since printing out file transfer completed when simulating target node faliue during assembly may look weird to prof
			fmt.Printf("\nChunk transfer completed with %d chunks.\n", len(chunks))
		} else {
			fmt.Println("\nFile transfer failed - no chunks created.")
		}
	} else {
		fmt.Println("\nTarget declined the file transfer.")
	}

	fmt.Println("\nReturning to main menu...")
	return nil
}

// Function to handle sender node failing before chunking (before sending chunk info) and before/during assembly (after sending chunk info)
func (n *Node) handleReceiverTimeout(senderIP string) {
	startTime := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(startTime).Seconds()

			// Check if the assembler has received the chunks
			chunks := n.checkAssemblerChunks()

			// fmt.Printf("Chunks and length: %v, %d", chunks, len(chunks))
			if len(chunks) > 0 {
				fmt.Printf("\nReceived %d chunks successfully\n", len(chunks))
				n.AssemblerChunks = []ChunkInfo{} // Clear the assembler chunks
				return
			}
			if elapsed >= 30 && len(chunks) == 0 {
				fmt.Println("\n\nRECEIVER TIMEOUT: No chunks received from sender in 30 seconds")
				fmt.Println("Sender node may have crashed.")
				fmt.Println("\nReturning to main menu...")
				return
			}

			fmt.Printf("\rs for chunks from sender... Time elapsed: %.0f seconds", elapsed)
		}
	}
}

// checkAssemblerChunks checks if the Assembler has received chunks
func (n *Node) checkAssemblerChunks() []ChunkInfo {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	// Return the current state of AssemblerChunks
	return n.AssemblerChunks
}

func (n *Node) ConfirmFileTransfer(request Message, reply *Message) error {
	fmt.Printf("Received file transfer request from %s for file %s\n", request.IP, request.FileName)
	var userResponse string
	fmt.Print("Do you want to receive the file? (yes/no):")
	fmt.Scanln(&userResponse)
	// fmt.Printf()
	// fmt.Printf("User response: %s\n", userResponse)
	if userResponse == "es" {
		go n.handleReceiverTimeout(request.IP)
		*reply = Message{
			Type: CONFIRM,
		}
	} else {
		*reply = Message{
			Type: REJECT,
		}
	}
	// If condition to handle sender node failing before chunking (before sending chunk info) and before/during assembly (after sending chunk info)
	// if userResponse == "es" {

	// }
	return nil
}

func (n *Node) FindSuccessor(message Message, reply *Message) error {
	// fmt.Printf("[NODE-%d] Finding successor for %d...\n", n.ID, message.ID)
	if utils.Between(message.ID, n.ID, n.Successor.ID, true) { // message.ID is between n.ID and n.Successor.ID (inclusive of Successor ID)

		// Check if the successor is alive
		_, err := CallRPCMethod(n.Successor.IP, "Node.Ping", Message{})
		if err != nil { // if the successor is not alive
			// fmt.Printf("[NODE-%d] Successor Node-%d appears to be down.\n", n.ID, n.Successor.ID)
			nextSuccessor := n.findNextAlive()

			if nextSuccessor == (Pointer{}) { // null pointer => no successor of node n is alive(very unlikely)
				fmt.Printf("[NODE-%d] No Successor from the successor list is alive.", n.ID)
				nextSuccessor = Pointer{ID: n.ID, IP: n.IP}
			}

			// fmt.Printf("[NODE-%d] next alive successor found %d\n", n.ID, nextSuccessor.ID)
			*reply = Message{
				ID: nextSuccessor.ID,
				IP: nextSuccessor.IP,
			}
			return nil
		}
		*reply = Message{
			ID: n.Successor.ID,
			IP: n.Successor.IP,
		}
		// fmt.Printf("[NODE-%d] Successor found: %v\n", n.ID, reply.ID)
		return nil
	} else {
		closest := n.closestPrecedingNode(message.ID)
		if closest.ID == n.ID {
			*reply = Message{
				ID: n.ID,
				IP: n.IP,
			}
			// fmt.Printf("[NODE-%d] Successor is self: %v\n", n.ID, reply.ID)
			return nil
		}
		newReply, _ := CallRPCMethod(closest.IP, "Node.FindSuccessor", message)
		*reply = *newReply
		return nil
	}
}

func (n *Node) closestPrecedingNode(id int) Pointer {
	for i := utils.M - 1; i >= 0; i-- {
		if utils.Between(n.FingerTable[i].ID, n.ID, id, false) {
			return n.FingerTable[i]
		}
	}
	return Pointer{ID: n.ID, IP: n.IP}
}

// Handled by the bootstrap node
func (n *Node) Join(joinIP string) {
	// Joining the network
	message := Message{
		Type: "Join",
		ID:   n.ID,
		IP:   n.IP,
	}

	reply, err := CallRPCMethod(joinIP, "Node.FindSuccessor", message)

	if err != nil {
		log.Fatalf("[NODE-%d] Failed to join network: %v", n.ID, err)
	}

	// fmt.Printf("[NODE-%d] Joining network with successor: %v\n", n.ID, reply.ID)
	n.Predecessor = Pointer{}
	n.Successor = Pointer{ID: reply.ID, IP: reply.IP}

	// Notify the successor of the new predecessor
	message = Message{
		Type: "NOTIFY",
		ID:   n.ID,
		IP:   n.IP,
	}

	_, err = CallRPCMethod(n.Successor.IP, "Node.Notify", message)
	if err != nil {
		log.Fatalf("[NODE-%d] Failed to notify successor: %v", n.ID, err)
	}
}

func (n *Node) Stabilize() {
	for {
		time.Sleep(timeInterval * time.Second)

		// fmt.Printf("[NODE-%d] Stabilizing...\n", n.ID)

		reply, err := CallRPCMethod(n.Successor.IP, "Node.GetPredecessor", Message{})
		if err != nil {
			nextSuccessor := n.findNextAlive()
			if nextSuccessor == (Pointer{}) {
				fmt.Printf("[NODE-%d] No Successor from the successor list is alive.", n.ID)
				fmt.Printf("[NODE-%d] Failed to get successor's predecessor: %v\n", n.ID, err)
				nextSuccessor = Pointer{ID: n.ID, IP: n.IP}
			}
			n.Successor = nextSuccessor
		} else {
			successorPredecessor := Pointer{ID: reply.ID, IP: reply.IP}
			if successorPredecessor != (Pointer{}) && utils.Between(successorPredecessor.ID, n.ID, n.Successor.ID, false) {
				n.Successor = successorPredecessor
				// fmt.Printf("[NODE-%d] Successor updated to %d\n", n.ID, n.Successor.ID)
			}
		}

		// Notify the successor of the new predecessor
		message := Message{
			Type: "NOTIFY",
			ID:   n.ID,
			IP:   n.IP,
		}
		_, err = CallRPCMethod(n.Successor.IP, "Node.Notify", message)

		if err != nil {
			fmt.Printf("[NODE-%d] Failed to notify successor: %v\n", n.ID, err)
		}

		// Update the successor list
		n.updateSuccessorList()
	}
}

func (n *Node) GetSuccessor(message Message, reply *Message) error {
	*reply = Message{
		ID: n.Successor.ID,
		IP: n.Successor.IP,
	}
	return nil
}

func (n *Node) GetSuccessorList(message Message, reply *Message) error {
	*reply = Message{
		ID:            n.Successor.ID,
		SuccessorList: n.SuccessorList,
	}
	return nil
}

func (n *Node) GetPredecessor(message Message, reply *Message) error {
	*reply = Message{
		ID: n.Predecessor.ID,
		IP: n.Predecessor.IP,
	}
	return nil
}

func (n *Node) Notify(message Message, reply *Message) error {
	// fmt.Printf("[NODE-%d] Notified by node %d...\n", n.ID, message.ID)
	if n.Predecessor == (Pointer{}) || utils.Between(message.ID, n.Predecessor.ID, n.ID, false) {
		n.Predecessor = Pointer{ID: message.ID, IP: message.IP}
		// fmt.Printf("[NODE-%d] Predecessor updated to %d\n", n.ID, n.Predecessor.ID)
	}
	return nil
}

func (n *Node) FixFingers() {
	for {
		time.Sleep((timeInterval + 2) * time.Second)

		for next := 0; next < utils.M; next++ {
			// Calculate the start of the finger interval
			start := (n.ID + int(math.Pow(2, float64(next)))) % int(math.Pow(2, float64(utils.M)))

			// fmt.Printf("[NODE-%d] Fixing finger %d for key %d\n", n.ID, next, start)
			// Find and update successor for this finger
			message := Message{ID: start}
			var reply Message
			err := n.FindSuccessor(message, &reply)
			if err != nil {
				fmt.Printf("[NODE-%d] Failed to find successor for finger %d: %v\n", n.ID, next, err)
				continue
			}
			// fmt.Printf("[NODE-%d] Found successor for key %d: %v\n", n.ID, start, reply.ID)

			n.FingerTable[next] = Pointer{ID: reply.ID, IP: reply.IP}
		}
	}
}

// Add the GetNodeInfo method here
func (n *Node) GetNodeInfo(args struct{}, reply *NodeInfo) error {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	reply.ID = n.ID
	reply.IP = n.IP
	reply.Successor = n.Successor
	return nil
}

// Potential failure: When the find successor function is called, it should check if the find successor is alive or not
// If the find successor is not alive, it should keeping checking the next successor until it finds an alive one(?)
func (n *Node) updateSuccessorList() {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	next := Pointer{n.ID, n.IP}
	n.SuccessorList = []Pointer{}
	for i := 0; i < r; i++ {
		successorInfo, err := CallRPCMethod(next.IP, "Node.GetSuccessor", Message{})
		if err != nil {
			// fmt.Printf("[NODE-%d] Failed to get successor %d: %v\n", n.ID, i, err)
			continue
		}
		next = Pointer{ID: successorInfo.ID, IP: successorInfo.IP}
		n.SuccessorList = append(n.SuccessorList, next)
	}
}

func (n *Node) findNextAlive() Pointer {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	if len(n.SuccessorList) <= 1 {
		return Pointer{}
	}

	for i := 1; i < r; i++ {
		reply, err := CallRPCMethod(n.SuccessorList[i].IP, "Node.Ping", Message{})
		if err == nil && reply != nil {
			return n.SuccessorList[i]
		}
	}
	return Pointer{}
}

func CreateNode(ip string) *Node {
	id := utils.Hash(ip) % int(math.Pow(2, float64(utils.M))) // Ensure ID is within [0, 2^m - 1]

	node := &Node{
		ID:            id,
		IP:            ip,
		Successor:     Pointer{ID: id, IP: ip},
		Predecessor:   Pointer{},
		FingerTable:   make([]Pointer, utils.M),
		SuccessorList: make([]Pointer, 0),
		Lock:          sync.Mutex{},
	}

	// Initialize finger table with self to prevent nil entries
	for i := 0; i < utils.M; i++ {
		node.FingerTable[i] = Pointer{ID: node.ID, IP: node.IP}
	}

	return node
}

func CallRPCMethod(ip string, method string, message Message) (*Message, error) {
	client, err := rpc.Dial("tcp", ip)
	if err != nil {
		return &Message{}, fmt.Errorf("[NODE-%d] Failed to connect to node at %s: %v", message.ID, ip, err)
	}
	defer client.Close()

	var reply Message
	err = client.Call(method, message, &reply)
	if err != nil {
		return &Message{}, fmt.Errorf("[NODE-%d] Failed to call method %s: %v", message.ID, method, err)
	}

	return &reply, nil
}

func (n *Node) RemoveChunksLocal(request Message, reply *Message) error {
	if request.ChunkTransferParams.Chunks == nil {
		return fmt.Errorf("no chunks provided for removal")
	}
	dataDir := request.DataDir

	for _, chunk := range request.ChunkTransferParams.Chunks {
		chunkFilePath := filepath.Join(dataDir, chunk.ChunkName)

		// Check if file exists before removal
		if _, err := os.Stat(chunkFilePath); os.IsNotExist(err) {
			// fmt.Printf("Chunk file %s does not exist, skipping file...\n", chunk.ChunkName)
			continue
		}

		err := os.Remove(chunkFilePath)
		if err != nil {
			//commenting this out for now since, this message will be printed out when the target node is down during assembly
			//fmt.Printf("Error deleting chunk file %s: %v\n", chunk.ChunkName, err)
		}
	}
	fmt.Printf("Deleted chunk files from " + dataDir + "\n")
	return nil
}

// RemoveChunksRemotely handles the RPC call to remove chunks from the target node or other nodes that hold individual chunks
func (n *Node) removeChunksRemotely(dataDir string, chunkInfo []ChunkInfo) error {
	if len(chunkInfo) == 0 {
		return fmt.Errorf("no chunks provided for removal")
	}

	message := Message{
		DataDir: dataDir,
		ChunkTransferParams: ChunkTransferRequest{
			Chunks: chunkInfo,
		},
	}

	// removing chunks in /local and /assemble folders
	if dataDir != dataFolder {
		_, err := CallRPCMethod(n.IP, "Node.RemoveChunksLocal", message)
		if err != nil {
			return fmt.Errorf("failed to remove chunks from successor: %v", err)
		}
		return nil
	}

	// removing remote /shared folders
	for _, v := range chunkInfo {
		var reply Message
		err := n.FindSuccessor(Message{ID: v.Key}, &reply)
		if err != nil {
			fmt.Printf("Failed to find successor: %v\n", err)
			continue
		}
		// Get the successor list of the node
		successorReply, err := CallRPCMethod(reply.IP, "Node.GetSuccessorList", Message{})
		if err != nil {
			fmt.Printf("Failed to get successor list: %v\n", err)
			continue
		}
		successorList := successorReply.SuccessorList
		listToDelete := []Pointer{{ID: reply.ID, IP: reply.IP}}

		listToDelete = append(listToDelete, successorList...)
		for _, successor := range listToDelete {
			_, err := CallRPCMethod(successor.IP, "Node.RemoveChunksLocal", message)
			if err != nil {
				//commenting this out for now since, this message will be printed out when the target node is down during assembly
				//fmt.Printf("Failed to remove chunk %s from node %d: %v\n", v.ChunkName, successor.ID, err)
			}
		}
	}

	fmt.Printf("Deleted chunk files from /shared for this file transfer operation.\n")
	return nil
}

func (n *Node) CheckPredecessor() {
	for {
		time.Sleep(timeInterval * time.Second)
		if n.Predecessor != (Pointer{}) {
			// Try to ping the predecessor
			_, err := CallRPCMethod(n.Predecessor.IP, "Node.Ping", Message{})
			if err != nil {
				// fmt.Printf("[NODE-%d] Predecessor (Node-%d) appears to be down: %v\n", n.ID, n.Predecessor.ID, err)

				// Clear predecessor pointer
				n.Lock.Lock()
				n.Predecessor = Pointer{}
				n.Lock.Unlock()

				fmt.Printf("[NODE-%d] Predecessor appears to be down. Predecessor pointer cleared\n", n.ID)
			}
		}
	}
}

func (n *Node) Ping(message Message, reply *Message) error {
	*reply = Message{
		ID: n.ID,
		IP: n.IP,
	}
	return nil
}

func getAllNodes(n *Node) ([]Pointer, error) {
	nodes := []Pointer{}
	visited := make(map[int]bool)
	currentID := n.ID
	currentIP := n.IP
	nodes = append(nodes, Pointer{ID: currentID, IP: currentIP})
	visited[currentID] = true
	currentSuccessor := n.Successor

	for {
		if currentSuccessor.ID == n.ID {
			// We've come full circle
			break
		}
		if visited[currentSuccessor.ID] {
			// We've already visited this node
			break
		}

		client, err := rpc.Dial("tcp", currentSuccessor.IP)
		if err != nil {
			return nil, err
		}
		var successorInfo NodeInfo
		err = client.Call("Node.GetNodeInfo", struct{}{}, &successorInfo)
		client.Close()
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, Pointer{ID: successorInfo.ID, IP: successorInfo.IP})
		visited[successorInfo.ID] = true
		currentSuccessor = successorInfo.Successor
	}
	return nodes, nil
}

func (n *Node) ForceExit(message Message, reply *Message) error {
	fmt.Println("Received force exit command. Node shutting down...")
	go func() {
		os.Exit(1)
	}()
	return nil
}
