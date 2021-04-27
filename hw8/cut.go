package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

type Edge struct {
	src, dst int32
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

	// FIXME do I really need 1024
	next := make(chan int32, 1024)
	go func() {
		for {
			var source, dest int32
			err = binary.Read(fd, binary.LittleEndian, &source)
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Printf("Error reading souce vertex: %v\n", err.Error())
				os.Exit(4)
			}

			err = binary.Read(fd, binary.LittleEndian, &dest)
			if err != nil {
				// no EOF check here, all sources must have a vertex
				fmt.Printf("Error reading dest vertex: %v\n", err.Error())
				os.Exit(4)
			}

			next <- source
			next <- dest
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
