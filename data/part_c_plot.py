import sys
import numpy as np
import matplotlib.pyplot as plt
plt.style.use('seaborn-deep')


def plot(bptree, heap, fanout):
    plt.bar(list(bptree.keys()), bptree.values(),
            color='g', label='B+ tree + Heap')
    plt.bar(list(heap.keys()), heap.values(), color='r', label='Only Heap')
    plt.legend(loc='upper left')
    plt.xlabel('Number of Block Accesses')
    plt.ylabel('Count')
    plt.title('Distribution of Block Accesses needed for FANOUT = ' + fanout)
    plt.show()


if __name__ == "__main__":
    filename = sys.argv[1]
    fanout = sys.argv[2]

    bptree = {}
    heap = {}

    with open(filename) as file:
        for line in file:
            block_accesses = line.split()

            if block_accesses[0] not in bptree.keys():
                bptree[block_accesses[0]] = 0

            if block_accesses[1] not in heap.keys():
                heap[block_accesses[1]] = 0

            bptree[block_accesses[0]] += 1
            heap[block_accesses[1]] += 1

        bptree = dict(sorted(bptree.items()))
        heap = dict(sorted(heap.items()))

        print(bptree)
        print(heap)
        plot(bptree, heap, fanout)
