/**
 * UnorderedHeap represents a linked list of data block nodes connected.
 * Records(or keys) are inserted into the heap in the first open space.
 * Deleted records are represented by DELETE_MARKERs (which is -1)
 *
 */

#ifndef BPTREE_UNORDEREDHEAP_HPP
#define BPTREE_UNORDEREDHEAP_HPP

#include "global.hpp"
#include "Block.hpp"

class UnorderedHeap {
public:
    BlockPtr first_block_ptr;

    explicit UnorderedHeap(BlockPtr block_ptr = NULL_PTR);

    RecordPtr insert_key(const Key& key) const;
    void delete_key(const Key& key);
    void export_heap() const;
    void range(ostream& os, const Key& min_key, const Key& max_key) const;
    void chart(ostream& os) const;

    friend ostream& operator<<(ostream& os, UnorderedHeap unordered_heap);
};


#endif //BPTREE_UNORDEREDHEAP_HPP
