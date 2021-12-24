/**
 * Data blocks contain keys. A data block can occupy at most BLOCK_SIZE records/keys.
 * Do not change this class.
 */

#ifndef BPTREE_BLOCK_HPP
#define BPTREE_BLOCK_HPP

#include "global.hpp"

typedef string BlockPtr;
typedef int Key;

class Block {
public:
    BlockPtr block_ptr;
    uint size;
    vector<Key> keys;
    BlockPtr next_block_ptr;

    explicit Block(const BlockPtr& block_ptr = NULL_PTR);

    bool is_full();
    bool is_empty() const;

    RecordPtr insert_key(const Key& key); //inserts key into block if there's space
    void insert_next_block(BlockPtr block_ptr); //links current block to block_ptr
    bool delete_key(const Key& key);//deletes key from block is exists
    void delete_block();//deletes block on disk
    void export_block(ostream& os);
    void range(ostream& os, const Key& min_key, const Key& max_key);
    void chart(ostream& os) const;

    void dump(); //writes block to disk

    bool operator==(const Block* other) const;
    bool operator==(BlockPtr block_ptr) const;
    Key operator[](int index) const;

    friend ostream& operator<<(ostream& os, Block* block);
    friend istream& operator>>(istream& is, Block* block);
};


#endif //BPTREE_BLOCK_HPP
