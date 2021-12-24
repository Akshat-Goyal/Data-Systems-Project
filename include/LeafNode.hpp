/**
 * LeafNode represents leaf nodes in the B+Tree. The leaf node contains
 * <key, RecordPtrs> pairs. Each leaf (except root) have between ceil(FANOUT/2)
 * and FANOUT children.
 */

#ifndef BPTREE_LEAFNODE_HPP
#define BPTREE_LEAFNODE_HPP

#include "TreeNode.hpp"
#include <map>

class LeafNode : public TreeNode {
public:
    map<Key, RecordPtr> data_pointers;
    TreePtr next_leaf_ptr;

    //constructors
    explicit LeafNode(const TreePtr& tree_ptr = NULL_PTR);

    //helper functions
    Key max() override;

    //key functions
    TreePtr insert_key(const Key &key, const RecordPtr &record_ptr) override;
    void delete_key(const Key &key) override;
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


#endif //BPTREE_LEAFNODE_HPP
