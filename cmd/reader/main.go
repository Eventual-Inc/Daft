package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	fbs "github.com/Eventual-Inc/Daft/codegen/go/Daft"
	flatbuffers "github.com/google/flatbuffers/go"
)

const DataSockAddr = "/run/eventual/data.sock"

func main() {
	listener, err := net.Listen("unix", DataSockAddr)
	if err != nil {
		log.Fatal(err)
	}

	defer listener.Close()

	c, err := listener.Accept()
	if err != nil {
		log.Fatal(err)
	}

	var buf bytes.Buffer

	for {
		c.SetReadDeadline(time.Now().Add(3 * time.Second))

		start := time.Now()
		// Read enough data to get the size prefix, which is a UInt32
		bytesRead := 0
		for bytesRead < 4 {
			tmpBytes := make([]byte, 4)
			nbytes, err := c.Read(tmpBytes)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error reading from socket: %v", err)
			}
			bytesRead += nbytes
			buf.Write(tmpBytes[0:nbytes])
		}
		sizePrefix := make([]byte, 4)
		buf.Read(sizePrefix)
		payloadSize := flatbuffers.GetUint32(sizePrefix)
		log.Printf("Found a payload of size: %d", payloadSize)

		// Read the entire flatbuffer payload
		for uint32(buf.Len()) < payloadSize {
			tmpBytes := make([]byte, payloadSize)
			nbytes, err := c.Read(tmpBytes)
			if err != nil {
				log.Fatalf("Error reading from socket: %v", err)
			}
			if err == io.EOF {
				break
			}
			buf.Write(tmpBytes[0:nbytes])
			log.Printf("Read bytes: %d", nbytes)
		}
		log.Printf("Finished reading entire payload")

		payload := make([]byte, payloadSize)
		nbytes, err := buf.Read(payload)
		if uint32(nbytes) != payloadSize || err != nil {
			log.Fatalf("Unable to read correct number of bytes from buffer with error: %v", err)
		}
		log.Printf("Time to read record from socket: %v", time.Since(start))
		start = time.Now()

		fileRecord := fbs.GetRootAsFile(payload, 0)
		log.Printf("Time to load record: %v", time.Since(start))
		start = time.Now()

		md5Hash := md5.Sum(fileRecord.DataBytes())
		log.Println(fmt.Sprintf("Pulled a new record with md5 hash: %x", md5Hash))
	}
}
