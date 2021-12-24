/**
 * RecordPtr points to the block and the record position of a particular
 * key.
 */

#ifndef BPTREE_RECORDPTR_HPP
#define BPTREE_RECORDPTR_HPP

#include "Block.hpp"

typedef uint RecordPosition;

class RecordPtr {
public:
    BlockPtr block_ptr;
    RecordPosition record_pos;

    explicit RecordPtr(BlockPtr block_ptr = NULL_PTR, RecordPosition record_pos = 0);

    void write_data(ostream& os) const; // accessed the data from block_ptr and writes to os

    friend ostream& operator<<(ostream& os, const RecordPtr& record_ptr);
    friend istream& operator>>(istream& is, RecordPtr& record_ptr);
};


#endif //BPTREE_RECORDPTR_HPP
