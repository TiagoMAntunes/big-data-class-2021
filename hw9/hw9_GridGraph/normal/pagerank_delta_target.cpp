#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <array>
#include <algorithm>
#include <iterator>

#include <sys/time.h>

inline double get_time() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec + (tv.tv_usec / 1e6);
}

#include <omp.h>

#define VERTEX_COUNT 4847571

typedef int VertexId;

int main(int argc, char *argv[])
{

    if (argc < 4)
    {
        printf("Usage: %s [path] [iterations] [threshold]\n", argv[0]);
        exit(-1);
    }

    std::string path = argv[1];
    int iterations = atoi(argv[2]);
    float threshold = std::stof(argv[3]);

    std::vector<std::vector<VertexId>> edges(VERTEX_COUNT, std::vector<VertexId>());

    std::vector<float> pagerank(VERTEX_COUNT);
    std::vector<float> sum(VERTEX_COUNT);
    std::vector<float> degree(VERTEX_COUNT);
    std::vector<float> intermediate(VERTEX_COUNT); // cache values
    std::vector<float> delta(VERTEX_COUNT);
    std::vector<bool>  set(VERTEX_COUNT);

    std::fill(pagerank.begin(), pagerank.end(), 1.f / VERTEX_COUNT);
    std::fill(sum.begin(), sum.end(), 0);
    std::fill(degree.begin(), degree.end(), 0);
    std::fill(intermediate.begin(), intermediate.end(), 0);
    std::fill(delta.begin(), delta.end(), 1);
    std::fill(set.begin(), set.end(), false);

    printf("Going to load graph. First element has value %0.20f, should be %0.20f\n", pagerank[0], 1.f / VERTEX_COUNT);
    FILE *fin = fopen(argv[1], "rb");
    
    while (true)
    {
        VertexId src, dst;
        if (fread(&src, sizeof(src), 1, fin) == 0)
            break;
        if (fread(&dst, sizeof(dst), 1, fin) == 0)
            break;
        edges[dst].push_back(src);
        degree[src]++;
    }
    fclose(fin);

    printf("Graph loaded, running pagerank...\n");

    double begin_time = get_time();

    for (int iter = 0; iter < iterations; iter++) {

        //scatter
        #pragma omp parallel for schedule(guided) 
        for (int dst = 0; dst < VERTEX_COUNT; dst++) { 
            float accumulator = 0;
            for (VertexId src : edges[dst]) {
                if (!set[src]) {
                    intermediate[src] = delta[src] / degree[src];
                    set[src] = true;
                }
                if (intermediate[src] > threshold)
                    accumulator += intermediate[src];
            }
            sum[dst] = accumulator;
        }

        //apply
        #pragma omp parallel for schedule(static)
        for (int src = 0; src < VERTEX_COUNT; src++) { 
            delta[src] = 0.85 * sum[src];
            pagerank[src] += delta[src];
            sum[src] = 0;
            intermediate[src] = 0;
            set[src] = false;
        }
    }
    double end_time = get_time();

	printf("%d iterations of pagerank took %.2f seconds\n", iterations, end_time - begin_time);
    auto max = std::max_element(pagerank.begin(), pagerank.end());
    printf("Max: %0.20f, index: %ld\n", *max, std::distance(pagerank.begin(), max));
}