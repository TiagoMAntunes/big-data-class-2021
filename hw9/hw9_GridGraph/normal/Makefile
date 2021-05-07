TARGETS= conductance pagerank_delta
CC=g++
FLAGS = -O3 -Wall -std=c++11 -fopenmp

all: $(TARGETS)

conductance: conductance.cpp
	$(CC) $(FLAGS) -o $@ $<

pagerank_delta: pagerank_delta.cpp
	$(CC) $(FLAGS) -o $@ $<

clean:
	rm pagerank_delta conductance