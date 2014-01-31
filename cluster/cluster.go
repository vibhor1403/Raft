//Package cluster provides interface for creating a new cluster and sending and recieving messages through sockets.
// It also provides message structure to be passed.
package cluster

import (
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"io/ioutil"
	"time"
)

const (
	BROADCAST = -1
	MAX       = 10
)

// Envelope describes the message structure to be followed to communicate with other servers.
type Envelope struct {
	//SendTo specifies the pid of the recieving system. Setting it to -1, will broadcast the message to all peers.
	SendTo int

	// SendBy specifies the pid of the sender.
	SendBy int

	// MsgId is an id that globally and uniquely identifies the message, meant for duplicate detection at
	// higher levels. It is opaque to this package.
	MsgId int64

	//Msg comprises of the actual message.
	Msg interface{}
}

// Server interface provides various methods for retriving information about the cluster.
type Server interface {
	// Pid is the Id of this server
	Pid() int

	// Peers contains array of other servers' ids in the same cluster
	Peers() []int

	// Outbox is the channel to use to send messages to other peers.
	Outbox() chan *Envelope

	// Inbox is the channel to receive messages from other peers.
	Inbox() chan *Envelope

	// MsgRcvd retrives the count of number of messages recieved by this system
	MsgRcvd() int
	
	// MsgSent retrives the count of number of messages sent by this system
	MsgSent() int
}


type jsonobject struct {
	Object ObjectType
}

type ObjectType struct {
	Total   int
	Servers []ServerConfig
}

//ServerConfig is structure containing all the information needed about this server
type ServerConfig struct {
	// Pid of this server
	Mypid   int
	// Url (ip:port) of this server
	Url     string
	// Input channel for holding incoming data
	Input   chan *Envelope
	// Output channel for sending data to other peers
	Output  chan *Envelope
	// Array of peers
	Mypeers []int
	// Array of all sockets opened by this server (contains 4 outbound sockets)
	Sockets []*zmq.Socket
	// N_msgRcvd is count of messages recieved by this server
	N_msgRcvd int
	// N_msgSent is count of messages sent by this server
	N_msgSent int
}

func (sc ServerConfig) Pid() int {
	return sc.Mypid
}

func (sc ServerConfig) Peers() []int {
	return sc.Mypeers
}

func (sc ServerConfig) Inbox() chan *Envelope {
	return sc.Input
}

func (sc ServerConfig) Outbox() chan *Envelope {
	return sc.Output
}

func (sc ServerConfig) MsgRcvd() int {
	return sc.N_msgRcvd
}

func (sc ServerConfig) MsgSent() int {
	return sc.N_msgSent
}

// mapping maps pid of the server to its url
var mapping map[int]string

// New function is the main function, which initializes all the parameters needed for server to function correctly. Further, it also
// starts routines to check for the channels concurrently..
func New(pid int, conf string) Server {

	file, e := ioutil.ReadFile(conf)
	if e != nil {
		panic ("Could not read file")
	}
	var jsontype jsonobject
	err := json.Unmarshal(file, &jsontype)
	if err != nil {
		panic ("Wrong format of conf file")
	}

	// Inialization of mapping and server parameters.
	mapping = make(map[int]string)
	sc := ServerConfig{
		Mypid:   pid,
		Url:     mapping[pid],
		Input:   make(chan *Envelope),
		Output:  make(chan *Envelope),
		Mypeers: make([]int, jsontype.Object.Total-1),
		Sockets: make([]*zmq.Socket, jsontype.Object.Total-1),
		N_msgRcvd: 0,
		N_msgSent: 0}

// Populates the peers of the servers, and opens the outbound ZMQ sockets for each of them.
	k := 0
	for i := 0; i < jsontype.Object.Total; i++ {
		mapping[jsontype.Object.Servers[i].Mypid] = jsontype.Object.Servers[i].Url

		if jsontype.Object.Servers[i].Mypid != pid {
			sc.Mypeers[k] = jsontype.Object.Servers[i].Mypid
			sc.Sockets[k], err = zmq.NewSocket(zmq.DEALER)

			if err != nil {
				panic (fmt.Sprintf("Unable to open socket for %v as %v", sc.Mypeers[k], err))
			}
			err = sc.Sockets[k].Connect("tcp://" + mapping[sc.Mypeers[k]])
			if err != nil {
				panic (fmt.Sprintf("Unable to connect socket for %v as %v", sc.Mypeers[k], err))
			}
			k++
		}
	}
	sc.Url = mapping[pid]

// 	Starts two go routines each for input and output channel functionalities..
//	go CheckInput(sc)
	go CheckOutput(sc)
	go Listen(sc)
	return sc
}

// CheckInput waits on input channel of the server and prints the data on standard output.
func CheckInput(sc ServerConfig) {
	for {
		envelope, ok := <-sc.Input
		if !ok {
			panic("channels closed..")
		}
		fmt.Printf("Received msg from %d to %d\n", envelope.SendBy, envelope.SendTo)
		sc.N_msgRcvd++
	}
}

// CheckOutput waits on output channel, and according to the type of message recieved on this channel sends the data to other peers.
func CheckOutput(sc ServerConfig) {
	for {
		x, ok := <-sc.Output
		// If Output channel is not closed.
		if ok {
			// If BROADCAST message
			if x.SendTo == -1 {
				for i := 0; i < len(sc.Mypeers); i++ {
					b, _ := json.Marshal(*x)
					_, err := sc.Sockets[i].Send(string(b), 0)
					if err != nil {
						panic (fmt.Sprintf("Could not send message,%v,%v..%v", sc.Mypid, sc.Mypeers[i], err))
					} else {
						sc.N_msgSent++
					}
				}
			} else {
				b, _ := json.Marshal(*x)
				_, err := sc.Sockets[findPid(sc, x.SendTo)].Send(string(b), 0)
				if err != nil {
					panic (fmt.Sprintf("Could not send 1message,%v,%v..%v", sc.Mypid, sc.Mypeers[findPid(sc, x.SendTo)], err))
				} else {
					sc.N_msgSent++
				}
			}
		// If output channel is closed, then it closes all the outbound sockets of the current server.
		} else {
			for i := 0; i < len(sc.Mypeers); i++ {
				sc.Sockets[i].Close()
			}
			return
		}
	}
}

// findPid returns the index of server's peers array which correspond to the given pid. If not found, returns -1
func findPid (sc ServerConfig, pid int) int {
	for i := 0; i < len(sc.Mypeers); i++ {
		if pid == sc.Mypeers[i] {
			return i
		}
	}
	return -1
}

// Listen method waits on recieving socket to gather data from the peers. Wait timeout is set to 2 seconds. If no data is available for
// more than 2 seconds, listen socket is closed, and correspondingly the input channel is also closed, which enables another routine
// waiting on input channel to be notified.
// This timeout can be set to -1 if we want that socket remains open for indefinite amount of time.
func Listen(sc ServerConfig) {
	listenSocket, er := zmq.NewSocket(zmq.DEALER)
	if er != nil {
		panic ("Unable to open socket for listening")
	}
	defer listenSocket.Close()
	listenSocket.Bind("tcp://" + sc.Url)
	listenSocket.SetRcvtimeo(2*time.Second)
	for {
		msg, err := listenSocket.Recv(0)
		if err != nil {
			close(sc.Input)
			return
		} else {
			sc.N_msgRcvd++
		}
		
		message := new(Envelope)
		json.Unmarshal([]byte(msg), message)
		sc.Input <- message
	}
}
