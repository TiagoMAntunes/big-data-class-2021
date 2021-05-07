#include "core/graph.hpp"

#define min(A, B) ((A) < (B) ? (A) : (B))

int main(int argc, char **argv)
{

    if (argc < 2)
    {
        fprintf(stderr, "usage: conductance [path] [memory budget in GB]\n");
        exit(-1);
    }

    std::string path = argv[1];
    long memory_bytes = atol(argv[2]) * 1024l * 1024l * 1024l;

    Graph graph(path);
    graph.set_memory_bytes(memory_bytes);

    int counter[] = {0, 0};

    double begin_time = get_time();

    int crossover_count = graph.stream_edges<VertexId>(
        [&](Edge &e) {
            if ((e.source & 1) != (e.target & 1))
                return 1;

            int index = (e.source & 1) == 0;
            write_add(counter + index, 1);
            return 0;
        });
    double end_time = get_time();

    printf("Conductance took %.2f seconds\n", end_time - begin_time);
    printf("Count: %d, red: %d, black: %d\n", crossover_count, counter[0], counter[1]);
    printf("Conductance: %0.5f\n", crossover_count / (float)min(counter[0], counter[1]));
}