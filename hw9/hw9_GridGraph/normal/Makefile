TARGETS= conductance pagerank_delta.run pagerank_delta_edges.run pagerank_delta_target.run pagerank_delta_target_sorted.run
CC=g++
FLAGS = -O3 -Wall -std=c++11 -fopenmp

all: $(TARGETS)

conductance: conductance.cpp
	$(CC) $(FLAGS) -o $@ $<

pagerank_delta.run: pagerank_delta.cpp
	$(CC) $(FLAGS) -o $@ $<

pagerank_delta_edges.run: pagerank_delta_edges.cpp
	$(CC) $(FLAGS) -o $@ $<

pagerank_delta_target.run: pagerank_delta_target.cpp
	$(CC) $(FLAGS) -o $@ $<

pagerank_delta_target_sorted.run: pagerank_delta_target_sorted.cpp
	$(CC) $(FLAGS) -o $@ $<

clean:
	rm $(TARGETS)