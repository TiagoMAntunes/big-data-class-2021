import sys
import numpy as np

if len(sys.argv) < 2:
    print(f'Usage: python3 {sys.argv[0]} <file to shuffle>')
    sys.exit(0)

edges = np.fromfile(sys.argv[1], dtype=np.int32).reshape(-1,2)

np.random.shuffle(edges)

edges.tofile(f'{sys.argv[1]}.shuffled')

print(edges[:50])
