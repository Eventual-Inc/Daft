package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/apache/arrow/go/arrow/ipc"
)

const DataSockAddr = "/run/eventual/data.sock"

func main() {
	listener, err := net.Listen("unix", DataSockAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	c, err := listener.Accept()
	reader, err := ipc.NewReader(c)

	if err != nil {
		log.Fatal(err)
	}
	for {
		start := time.Now()
		rec, err := reader.Read()
		log.Printf("Time to read record from socket: %v", time.Since(start))
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Println(fmt.Sprintf("Pulled a new record with column 0: %v", rec.ColumnName(0)))
	}
}
