/**
 * BPTree represents a B+Tree over the keys in the unordered heap.
 * Do not change this class.
 */

#ifndef BPTREE_BPTREE_HPP
#define BPTREE_BPTREE_HPP

#include "TreeNode.hpp"
#include "InternalNode.hpp"
#include "LeafNode.hpp"

class BPTree {
public:
    TreePtr root_ptr; //pointer to root node of tree

    explicit BPTree(const TreePtr& tree_ptr = NULL_PTR);

    void insert_key(const Key& key, const RecordPtr& record_ptr); //inserts key and record pointer into bptree
    void delete_key(const Key& key);//delete key from bptree if exists
    void range(ostream& os, const Key& min_key, const Key& max_key) const;
    void export_bptree() const;
    void chart(ostream& os) const;
};


#endif //BPTREE_BPTREE_HPP
