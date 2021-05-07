#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <array>
#include <algorithm>
#include <iterator>

#include <sys/time.h>
#define min(A, B) ((A) < (B) ? (A) : (B))

inline double get_time()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + (tv.tv_usec / 1e6);
}

#include <omp.h>

#define VERTEX_COUNT 4847571

typedef int VertexId;

int main(int argc, char *argv[])
{

    if (argc < 2)
    {
        printf("Usage: %s [path]", argv[0]);
        exit(-1);
    }

    std::string path = argv[1];

    int crossover_count = 0;
    int counter[] = {0, 0};

    FILE *fin = fopen(argv[1], "rb");
    double begin_time = get_time();

    while (true)
    {
        VertexId src, dst;
        if (fread(&src, sizeof(src), 1, fin) == 0)
            break;
        if (fread(&dst, sizeof(dst), 1, fin) == 0)
            break;

        if ((src & 1) != (dst & 1))
            crossover_count++;
        else
        {
            int index = (src & 1) == 0;
            counter[index]++;
        }
    }

    double end_time = get_time();
    fclose(fin);

    printf("Conductance took %.2f seconds\n", end_time - begin_time);
    printf("Count: %d, red: %d, black: %d\n", crossover_count, counter[0], counter[1]);
    printf("Conductance: %0.5f\n", crossover_count / (float)min(counter[0], counter[1]));
}