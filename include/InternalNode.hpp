/**
 * InternalNode represents internal nodes in the B+Tree. Every internal node
 * has a list of keys and tree pointers. An internal node contains a list of keys
 * and tree pointers(children) of the form
 *
 *    <ptr1, K1, ptr2, K2, ..., Kq-1, ptrq>
 *
 * where the subtree rooted at ptri contains data points that are greater than
 * Ki-1 and less than or equal to Ki i.e. Ki-1 < ptri <= Ki
 * Every internal node (except the root) should have between ceil(FANOUT/2)
 * and FANOUT children.
 */

#ifndef BPTREE_INTERNALNODE_HPP
#define BPTREE_INTERNALNODE_HPP

#include "TreeNode.hpp"

class InternalNode : public TreeNode {
public:
    vector<Key> keys;
    vector<TreePtr> tree_pointers;

    //constructors
    explicit InternalNode(const TreePtr& tree_ptr = NULL_PTR);

    //helper functions
    Key max() override;
    TreePtr single_child_ptr() override;

    //key functions
    TreePtr insert_key(const Key& key, const RecordPtr& record_ptr) override;
    void delete_key(const Key& key) override;
    void left_redistribute(TreeNode*) override;
    void right_redistribute(TreeNode*) override;
    void merge_node(TreeNode*) override;
    void range(ostream& os, const Key& min_key, const Key& max_key) const override;
    void export_node(ostream& os) override;
    void chart(ostream& os) override;

    //I/O
    ostream& write(ostream& os) const override;
    istream& read(istream& is) override;
};


#endif //BPTREE_INTERNALNODE_HPP
