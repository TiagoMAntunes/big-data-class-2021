package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

type Edge struct {
	src, dst uint32
}

func main() {
	args := os.Args

	if len(args) < 4 {
		fmt.Printf("Usage: %v <edge|vertex|hybrid|greedy> <#machines> <filename>\n", args[0])
		os.Exit(1)
	}

	switch args[1] {
	case "edge":
	case "vertex":
	case "hybrid":
	case "greedy":
	default:
		fmt.Printf("Unrecognized splitting method. Available: edge, vertex, hybrid, greedy\n")
		os.Exit(2)
	}

	nMachines, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Printf("Invalid number of machines, please input an integer number\n")
		os.Exit(3)
	}

	fmt.Printf("Split method = %v, #machines = %v\n", args[1], nMachines)

	fd, err := os.Open(args[3])
	if err != nil {
		fmt.Printf("Error when opening file: %v\n", err.Error())
		os.Exit(4)
	}

	next := make(chan uint32, 1024)
	go func() {
		r := bufio.NewReader(fd)
		var buf [1024]byte
		stop := false
		for !stop {
			amount, err := io.ReadFull(r, buf[:])

			if err != nil {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					panic(err)
				}
				stop = true
			}

			for i := 0; i < amount/4; i++ {
				next <- binary.LittleEndian.Uint32(buf[i*4 : (i+1)*4])
			}

		}
		close(next)
	}()

	vertices := []Edge(nil)
	for {
		v1, more := <-next
		if !more {
			break
		}
		v2 := <-next
		// fmt.Printf("Pair: %v, %v\n", v1, v2)
		vertices = append(vertices, Edge{v1, v2})
	}

	fmt.Printf("All done! Number of vertices: %v\n", len(vertices))

}
