package cluster


/*
All the testing is done keeping json file with 5 server configuration and pid's from 1-5
Below is the json sample file, between the lines containing *

**********************************************************************************************
{"object": 
    	{
       		"total": 5,
       		"Servers":
       		[
               		{
                       		"mypid": 1,
                       		"url": "127.0.0.1:100001"
                       	},
			{
                       		"mypid": 2,
                       		"url": "127.0.0.1:100002"
                       	},
			{
                       		"mypid": 3,
                       		"url": "127.0.0.1:100003"
                       	},
			{
                       		"mypid": 4,
                       		"url": "127.0.0.1:100004"
                       	},
			{
                       		"mypid": 5,
                       		"url": "127.0.0.1:100005"
                       	}
       		]
    	}
}
****************************************************************************************
*/

import (
	"encoding/json"
	"github.com/vibhor1403/Raft/cluster"
	"math/rand"
	"testing"
	"sync"
//	"fmt"
	"time"
)

//Load testing... Tested for more than 130000 messages
func Test_Cluster_Load(t *testing.T) {
	total := 5
	var server [5]cluster.Server
	for i := 0; i < total; i++ {
		server[i] = cluster.New(i+1, "../config.json")
	}

	// Random messages
	message := []string{"hello", "hi", "how are you", "hello its long day", "hoho"}

	//Populating pids array with all pids including -1
	peers := server[0].Peers()
	n := len(peers)
	pids := make([]int, n+2)
	for i := 0; i < n; i++ {
		pids[i] = peers[i]
	}
	pids[n] = server[0].Pid()
	pids[n+1] = -1

	N_msg := 0

	count := make([]int, 5)
	for i := 0; i< total; i++ {
		count[i] = 0
	}

	wg := new(sync.WaitGroup)
	for i := 0; i< total; i++ {
		wg.Add(1)
		go checkInput(server[i], &count[i], wg)
	}

	for j := 0; j < 10; j++ {
		// Random sender
		x := rand.Intn(total)
		// Random reciever (pids[y])
		y := rand.Intn(n + 2)
		// Random message
		z := rand.Intn(5)

		if pids[y] == (x + 1) {
			continue
		}
		// Calculating total messages to be sent ideally.
		if pids[y] == -1 {
			N_msg += 4
		} else {
			N_msg++
		}
		// Sending message to channel
		server[x].Outbox() <- &cluster.Envelope{SendTo: pids[y], SendBy: x + 1, Msg: message[z]}
	}

	for i := 0; i< total; i++ {
		close(server[i].Outbox())
	}
	// Waiting for goroutines to end
	wg.Wait()

	var totalCount int
	for i := 0; i< total; i++ {
		totalCount += count[i]
	}

	if totalCount != N_msg {
		panic ("All messages not recieved..")
	}

	t.Log("Load test passed.")

}


func checkInput(server cluster.Server, count *int, wg *sync.WaitGroup){
	for {
		_, ok := <- server.Inbox()
		if !ok {
			wg.Done()
			return
		}
		*count++
	}
}

func Test_Cluster_Broadcast_Only(t *testing.T) {
	total := 5
	var server [5]cluster.Server
	for i := 0; i < total; i++ {
		server[i] = cluster.New(i+1, "../config.json")
	}

	// Random messages
	message := []string{"hello", "hi", "how are you", "hello its long day", "hoho"}

	//Populating pids array with all pids including -1
	peers := server[0].Peers()
	n := len(peers)
	pids := make([]int, n+2)
	for i := 0; i < n; i++ {
		pids[i] = peers[i]
	}
	pids[n] = server[0].Pid()
	pids[n+1] = -1

	N_msg := 0

	count := make([]int, 5)
	for i := 0; i< total; i++ {
		count[i] = 0
	}

	wg := new(sync.WaitGroup)
	for i := 0; i< total; i++ {
		wg.Add(1)
		go checkInput(server[i], &count[i], wg)
	}

	for j := 0; j < 1000; j++ {
		// Random sender
		x := rand.Intn(total)
		// Random message
		z := rand.Intn(5)

		// Calculating total messages to be sent ideally.

		N_msg += 4

		// Sending message to channel
		server[x].Outbox() <- &cluster.Envelope{SendTo: -1, SendBy: x + 1, Msg: message[z]}
	}

	for i := 0; i< total; i++ {
		close(server[i].Outbox())
	}
	wg.Wait()

	var totalCount int
	for i := 0; i< total; i++ {
		totalCount += count[i]
	}

	if totalCount != N_msg {
		panic ("All messages not recieved..")
	}

	t.Log("Broadcast Load test passed.")

}

// For testing cyclic data
func Test_Cluster_Cyclic_Dependencies(t *testing.T) {
	total := 5
	var server [5]cluster.Server
	for i := 0; i < total; i++ {
		server[i] = cluster.New(i+1, "../config.json")
	}

	// Random messages
	message := "hello"

	count := make([]int, 5)
	for i := 0; i< total; i++ {
		count[i] = 0
	}

	wg := new(sync.WaitGroup)
	for i := 0; i< total; i++ {
		wg.Add(1)
		go checkInput(server[i], &count[i], wg)
	}
	
	iterations := 1000
	for j := 0; j < iterations; j++ {
		for k :=0; k < total; k++ {
			server[k].Outbox() <- &cluster.Envelope{SendTo: (k+1)%total + 1, SendBy: k+1, Msg: message}
		}
	}

	for i := 0; i< total; i++ {
		close(server[i].Outbox())
	}
	wg.Wait()

	for i := 0; i< total; i++ {
		if count[i] != iterations {
			panic ("All messages not recieved..")
		}
	}

	t.Log("Cyclic message test passed.")
}


// For testing cyclic data
func Test_Cluster_Availability(t *testing.T) {
	total := 5
	var server [5]cluster.Server
	// server[4] is not started now.. (corresponding to pid=5)
	for i := 0; i < total-1; i++ {
		server[i] = cluster.New(i+1, "../config.json")
	}

	// Random messages
	message := "hello"

	count := make([]int, 5)
	for i := 0; i< total; i++ {
		count[i] = 0
	}
	
	for k :=0; k < total-1; k++ {
		server[k].Outbox() <- &cluster.Envelope{SendTo: -1, SendBy: k+1, Msg: message}
	}

	time.Sleep(time.Second)

	server[total-1] = cluster.New(total, "../config.json")

	wg := new(sync.WaitGroup)
	for i := 0; i< total; i++ {
		wg.Add(1)
		go checkInput(server[i], &count[i], wg)
	}

	for i := 0; i< total; i++ {
		close(server[i].Outbox())
	}
	wg.Wait()


	if count[4] != 4 {
		panic ("All messages not recieved..")
	}

	t.Log("test of Availability of cluster passed.")
}

//For testing large messages.....
func Test_Cluster_Message_Length(t *testing.T) {

	total := 5
	var server [5]cluster.Server
	for i := 0; i < total; i++ {
		server[i] = cluster.New(i+1, "../config.json")
	}

	message := make([]byte, 1000000)
	x := &cluster.Envelope{SendTo: -1, SendBy: 1, Msg: message}
	//Broadcasting long data
	server[0].Outbox() <- x

	for i := 0; i< total; i++ {
		close(server[i].Outbox())
	}
	b, _ := json.Marshal(*x)

	// Calculating if all servers recieve the data fully...
	for i := 1; i < total; i++ {
		envelope := <-server[i].Inbox()
		c, _ := json.Marshal(*envelope)
		if len(c) != len(b) {
			panic("Message not recieved fully..")
		}
	}

	t.Log("Length test passed.")
}

