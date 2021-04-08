#include "your_reduce.h"
#include <string.h>
#include <stdlib.h>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <omp.h>
// You may add your functions and variables here

void sum(int *a, const int *b, int count)
{
    // sum vectors a and b
    // final result is in a

    for (int i = 0; i < count; i++)
        a[i] += b[i];
}

void YOUR_Reduce(const int *sendbuf, int *recvbuf, int count)
{

    int size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int s = 1;

    int SIZE = count * sizeof(int);

    // need one extra array
    int *save = NULL;
    int *extra = NULL;

    int *fill_array = NULL;

	if (rank % 2 == 0) {
		// printf("Rank %d allocating extra space\n", rank);
		// first iteration will need to receive data
		// need one extra array
		save = (int *)malloc(SIZE);
		extra = (int *)malloc(SIZE);

		fill_array = extra;

		memset(extra, 0, SIZE);
	}

    int lvl = 1;

    MPI_Request req;
    bool received = false;

    while (s < size)
    { // logarithmic depth
        // Distance is given based on the depth
        int mod = s << 1;

        if (rank % mod)
        {                                  // not a collector
            int target = rank / mod * mod; // hack! just get the lower bound from that group
			// printf("lvl=%d %d sending to %d\n", lvl, rank, target);
            // send the result to target
            if (received)
            {
                MPI_Wait(&req, MPI_STATUS_IGNORE);
                sum(save, fill_array, count);
                MPI_Send(save, count, MPI_INT, target, 0, MPI_COMM_WORLD);
            }
            else
                MPI_Send(sendbuf, count, MPI_INT, target, 0, MPI_COMM_WORLD);
			// printf("lvl=%d %d finished sending to %d\n", lvl, rank, target);
            break; // doesn't have to do anything else...
        }
        else
        {
            if (s == 1) {
				// printf("Rank %d copying sendbuf first iteration\n", rank);
			    std::memcpy(save, sendbuf, SIZE); // this will always be needed
				// printf("Rank %d finished copying\n", rank);
			}
            // must collect the data and sum
            
            if (rank + s < size)
            {
                if (fill_array == recvbuf)
                    fill_array = extra;
                else
                    fill_array = recvbuf;
                if (s > 1)
                    MPI_Wait(&req, MPI_STATUS_IGNORE);
				// printf("lvl=%d %d receiving from %d\n", lvl, rank, rank+s);
                MPI_Irecv(fill_array, count, MPI_INT, rank + s, 0, MPI_COMM_WORLD, &req);
                received = true;
                if (fill_array == recvbuf)
                    // sum extra
                    sum(save, extra, count);
                else
                    // sum recvbuf
                    sum(save, recvbuf, count);

            }
        }
        lvl++;
        s = s << 1;
    }
    if (rank == 0)
    {
        if (received)
            MPI_Wait(&req, MPI_STATUS_IGNORE);
        sum(save, fill_array, count);
        
        memcpy(recvbuf, save, SIZE);
    }

	if (save)
    	free(save);
	if (extra)
    	free(extra);
    // printf("Node %d exiting...\n", rank);
}
