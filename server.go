package main

import (
	"bufio"
	"fmt"
	"github.com/vibhor1403/assignment2/cluster"
	"os"
	"flag"
)

func main() {

	// parse argument flags and get this server's id into myid
	mypid := flag.Int("pid", 0, "Pid of my own system")
	flag.Parse()
	server := cluster.New(*mypid /* config file */, "./config.json")

	// wait for keystroke to start.
	r := bufio.NewReader(os.Stdin)
	_, err := r.ReadString('\n')
	if err != nil {
		os.Exit(1)
	}

	// Let each server broadcast a message
	server.Outbox() <- &cluster.Envelope{SendTo: -1, SendBy: *mypid, Msg: "hello there"}
	close(server.Outbox())
	var input string
	fmt.Scanln(&input)
}


