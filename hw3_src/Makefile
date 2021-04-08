MPICXX=$(shell which mpicxx)

$(info Using mpicxx at $(MPICXX))

all: your_reduce.h your_reduce.cpp main.cpp
	mpicxx -o reduce -O2 -g -std=c++11 -lpthread -fopenmp your_reduce.cpp main.cpp
clean:
	rm reduce
