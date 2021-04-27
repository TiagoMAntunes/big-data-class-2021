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

type Partition struct {
	edgeCount, mirrorEdgeCount int
	mirror, master             map[uint32]bool
}

func edge(nMachines int, next chan uint32) {
	partitions := make([]Partition, nMachines)
	for i := range partitions {
		partitions[i].edgeCount = 0
		partitions[i].mirrorEdgeCount = 0
		partitions[i].mirror = make(map[uint32]bool)
		partitions[i].master = make(map[uint32]bool)
	}
	for {
		src, more := <-next
		if !more {
			break
		}
		dst := <-next

		i1 := src % uint32(nMachines)
		i2 := dst % uint32(nMachines)

		// update src info
		partitions[i1].edgeCount++
		partitions[i1].master[src] = true

		// update dst info
		partitions[i2].edgeCount++
		partitions[i2].master[dst] = true

		if i1 != i2 {
			// mirrors and replicated edges
			partitions[i1].mirror[dst] = true
			partitions[i1].mirrorEdgeCount++

			partitions[i2].mirror[src] = true
			partitions[i2].mirrorEdgeCount++
		} else {
			// both are masters, avoid duplicated edge count
			partitions[i1].edgeCount--
		}
	}

	// output
	for i := 0; i < nMachines; i++ {
		fmt.Printf("Partition %v\n%v\n%v\n%v\n%v\n\n", i, len(partitions[i].master), len(partitions[i].master)+len(partitions[i].mirror), partitions[i].mirrorEdgeCount, partitions[i].edgeCount)
		// fmt.Printf("%v %v\n", partitions[i].master, partitions[i].mirror)
	}
}

func vertex(nMachines int, next chan uint32) {
	partitions := make([]Partition, nMachines)
	for i := range partitions {
		partitions[i].edgeCount = 0
		partitions[i].mirror = make(map[uint32]bool)
		partitions[i].master = make(map[uint32]bool)
	}

	counter := 0 // keeps track of the current edge index
	for {
		src, more := <-next
		if !more {
			break
		}
		dst := <-next
		// fmt.Printf("%v %v\n", src, dst)
		// get the machine given the index
		index := counter % nMachines
		counter++

		// count increase
		partitions[index].edgeCount++

		i1 := src % uint32(nMachines)
		i2 := dst % uint32(nMachines)
		// a vertex is a master in its own hash ALWAYS
		partitions[i1].master[src] = true
		partitions[i2].master[dst] = true

		// a vertex is a mirror in this edge hash if it's not its own hash
		if int(i1) != index {
			partitions[index].mirror[src] = true
		}

		if int(i2) != index {
			partitions[index].mirror[dst] = true
		}
	}

	// output
	for i := 0; i < nMachines; i++ {
		fmt.Printf("Partition %v\n%v\n%v\n%v\n\n", i, len(partitions[i].master), len(partitions[i].master)+len(partitions[i].mirror), partitions[i].edgeCount)
		// fmt.Printf("%v %v\n", partitions[i].master, partitions[i].mirror)
	}
}

func greedy(nMachines int, next chan uint32) {
	partitions := make([]Partition, nMachines)
	for i := range partitions {
		partitions[i].edgeCount = 0
		partitions[i].mirror = make(map[uint32]bool)
		partitions[i].master = make(map[uint32]bool)
	}

	src_machines := make([]bool, nMachines)
	dst_machines := make([]bool, nMachines)

	for {
		for i := range src_machines {
			src_machines[i] = false
			dst_machines[i] = false
		}

		src, more := <-next
		if !more {
			break
		}
		dst := <-next

		// get machines where vertex is replicated
		for i, v := range partitions {
			_, src_ok1 := v.master[src]
			_, src_ok2 := v.mirror[src]
			if src_ok1 || src_ok2 {
				src_machines[i] = true
			}

			_, dst_ok1 := v.master[dst]
			_, dst_ok2 := v.mirror[dst]
			if dst_ok1 || dst_ok2 {
				dst_machines[i] = true
			}

		}

		// Case 1: if there is an intersection, use it
		machine := -1
		for i := range partitions {
			if src_machines[i] && dst_machines[i] {
				// found it
				machine = i
				break
			}
		}

		if machine != -1 {
			// use it
		}

		// check assigned vertices
		src_status := false
		dst_status := false
		for i := 0; i < nMachines && !(src_status && dst_status); i++ {
			if src_machines[i] {
				src_status = true
			}

			if dst_machines[i] {
				dst_status = true
			}
		}

		switch {
		case src_status && dst_status:
			// Case 2: if no intersection, get machine with the least edgeCount
			var leastCount uint32 = ^uint32(0)
			least := -1
			for i := 0; i < nMachines; i++ {
				if (src_machines[i] || dst_machines[i]) && partitions[i].edgeCount < int(leastCount) {
					leastCount = uint32(partitions[i].edgeCount)
					least = i
				}
			}

			least = least
		case src_status || dst_status:
			// Case 3: if only one vertice has been assigned, use that machine

		default:
			// Case 4: no vertex was assigned, use machine with the least edgeCount
		}

	}

	// output
	for i := 0; i < nMachines; i++ {
		fmt.Printf("Partition %v\n%v\n%v\n%v\n\n", i, len(partitions[i].master), len(partitions[i].master)+len(partitions[i].mirror), partitions[i].edgeCount)
		// fmt.Printf("%v %v\n", partitions[i].master, partitions[i].mirror)
	}
}

func main() {
	args := os.Args

	if len(args) < 4 {
		fmt.Printf("Usage: %v <edge|vertex|hybrid|greedy> <#machines> <filename>\n", args[0])
		os.Exit(1)
	}

	nMachines, err := strconv.Atoi(args[2])
	if err != nil {
		panic(err)
	}

	fmt.Printf("Split method = %v, #machines = %v\n", args[1], nMachines)

	fd, err := os.Open(args[3])
	if err != nil {
		panic(err)
	}

	// read the file and put each vertex found into the channel
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

	var function func(int, chan uint32)
	switch args[1] {
	case "edge":
		function = edge
	case "vertex":
		function = vertex
	case "hybrid":
	case "greedy":
		function = greedy
	default:
		panic("Unrecognized splitting method. Available: edge, vertex, hybrid, greedy")

	}

	function(nMachines, next)

	// vertices := []Edge(nil)
	// for {
	// 	v1, more := <-next
	// 	if !more {
	// 		break
	// 	}
	// 	v2 := <-next
	// 	// fmt.Printf("Pair: %v, %v\n", v1, v2)
	// 	vertices = append(vertices, Edge{v1, v2})
	// }

	// fmt.Printf("All done! Number of vertices: %v\n", len(vertices))

}
