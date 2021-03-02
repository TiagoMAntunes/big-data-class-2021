all: default

default: page_rank.cpp main.cpp
	g++ -std=c++11 -fopenmp -g  -Wall -o pr main.cpp page_rank.cpp common/graph.cpp
clean:
	rm -rf pr *~ *.*~
