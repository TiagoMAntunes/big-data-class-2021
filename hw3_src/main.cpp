#include <cstdio>
#include <cassert>
#include <cstdlib>
#include <ctime>
#include <random>
#include <sys/time.h>
#include <cstring>
#include "your_reduce.h"
#include "omp.h"

#define MAX_LEN 268435456

// return the time in the unit of us
static long get_time_us()
{
    struct timeval my_time;
    gettimeofday(&my_time, NULL);
    long runtime_us = 1000000 * my_time.tv_sec + my_time.tv_usec;
    return runtime_us;
}

int main(int argc, char *argv[])
{
    int size, rank, provided;

    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided); // enable multi-thread support (for Bonus)
    assert(provided == MPI_THREAD_MULTIPLE);

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int *a, *b; // the array used to do the reduction

    int *res;  // the array to record the result of YOUR_Reduce
    int *res2; // the array to record the result of MPI_Reduce

    long count;
    long begin_time, end_time, use_time, use_time2; // use_time for YOUR_Reduce & use_time2 for MPI_Reduce

    int i;

    // initialize
    a = (int *)malloc(MAX_LEN * sizeof(int));
    b = (int *)malloc(MAX_LEN * sizeof(int));
    res = (int *)malloc(MAX_LEN * sizeof(int));
    res2 = (int *)malloc(MAX_LEN * sizeof(int));
    memset(a, 0, sizeof(a));
    memset(b, 0, sizeof(b));
    memset(res, 0, sizeof(res));
    memset(res2, 0, sizeof(res2));

    std::mt19937 rng;
    rng.seed(time(NULL)); // seed to generate the array randomly
    // omp_set_dynamic(0);
    for (count = MAX_LEN; count <= MAX_LEN; count *= 8) // length of array : [ 1  16  256  4'096  65'536  1'048'576  16'777'216  268'435'456 ]
    // do not report results for length 1
    {
        for (int m = 0; m < 4; m++)
        {
            if (rank == 0)
                printf("Running with %d threads\n", m+1);
            // std::cout << "Running with " << m+1 << " threads" << std::endl;
            omp_set_num_threads(m+1);

            // the element of array is generated randomly
            for (i = 0; i < count; i++)
            {
                b[i] = a[i] = rng() % MAX_LEN;
            }

            // MPI_Reduce and then print the usetime, the result will be put in res2[]
            MPI_Barrier(MPI_COMM_WORLD);
            begin_time = get_time_us();
            MPI_Reduce(a, res2, count, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
            MPI_Barrier(MPI_COMM_WORLD);
            end_time = get_time_us();
            use_time2 = end_time - begin_time;
            if (rank == 0)
                printf("%ld int use_time : %ld us [MPI_Reduce]\n", count, use_time2), fflush(stdout);

            // YOUR_Reduce and then print the usetime, the result should be put in res[]
            MPI_Barrier(MPI_COMM_WORLD);
            begin_time = get_time_us();

            YOUR_Reduce(b, res, count);

            MPI_Barrier(MPI_COMM_WORLD);
            end_time = get_time_us();
            use_time = end_time - begin_time;
            if (rank == 0)
                printf("%ld int use_time : %ld us [YOUR_Reduce]\n", count, use_time), fflush(stdout);

            // check the result of MPI_Reduce and YOUR_Reduce
            if (rank == 0)
            {
                int correctness = 1;
                for (i = 0; i < count; i++)
                {
                    if (res2[i] != res[i])
                    {
                        correctness = 0;
                    }
                }
                if (correctness == 0)
                    printf("WRONG !!!\n"), fflush(stdout);
                else
                    printf("CORRECT !\n"), fflush(stdout);
            }
        }
    }
    MPI_Finalize();

    return 0;
}
