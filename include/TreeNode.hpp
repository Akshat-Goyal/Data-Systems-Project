/**
 * TreeNode represents a tree node in the B+Tree. The TreeNode is
 * a base class for both internal and leaf nodes.
 */

#ifndef BPTREE_TREENODE_HPP
#define BPTREE_TREENODE_HPP

#include "Block.hpp"


typedef BlockPtr TreePtr;
enum NodeType{
    LEAF,
    INTERNAL
};

ostream& operator<<(ostream& os, const NodeType& node_type);
istream& operator>>(istream& is, NodeType& node_type);

class TreeNode {
public:
    NodeType node_type;
    TreePtr tree_ptr;
    uint size;

    //constructor
    explicit TreeNode(const NodeType& node_type = INTERNAL, const TreePtr & tree_ptr = NULL_PTR);

    //generators
    static TreeNode* tree_node_factory(NodeType node_type);
    static TreeNode* tree_node_factory(TreePtr tree_ptr);
    static TreeNode* tree_node_factory(const TreePtr& child_tree_ptr1, const TreePtr& child_tree_ptr2);

    //helper functions
    bool is_empty() const;
    bool is_full() const;
    bool overflows() const;
    bool underflows() const;
    virtual Key max() {return DELETE_MARKER;}
    virtual TreePtr single_child_ptr() {return NULL_PTR;}

    //key functions
    virtual TreePtr insert_key(const Key& key, const RecordPtr& record_ptr) {return NULL_PTR;}
    virtual void delete_key(const Key& key) {}
    virtual void left_redistribute(TreeNode*) {};
    virtual void right_redistribute(TreeNode*) {};
    virtual void merge_node(TreeNode*) {};
    virtual void range(ostream& os, const Key& min_key, const Key& max_key) const {};
    virtual void chart(ostream& os) {}
    virtual void export_node(ostream& os);

    //I/O
    void dump() const;
    void load();
    void delete_node() const;
    virtual ostream& write(ostream& os) const;
    virtual istream& read(istream& is);

    friend ostream& operator<<(ostream& os, TreeNode* tree_node);
    friend istream& operator>>(istream& is, TreeNode* tree_node);
};


#endif //BPTREE_TREENODE_HPP
