#include "BPTree.hpp"

//creates bptree rooted at tree_ptr or creates a new bptree
BPTree::BPTree(const TreePtr &tree_ptr) {
    if(is_null(tree_ptr)){
        TreeNode* root_node = TreeNode::tree_node_factory(LEAF);
        this->root_ptr = root_node->tree_ptr;
        delete root_node;
    } else {
        this->root_ptr = tree_ptr;
    }
}

//inserts <key, record_ptr> into bptree
void BPTree::insert_key(const Key &key, const RecordPtr &record_ptr) {
    TreeNode* root_node = TreeNode::tree_node_factory(this->root_ptr);
    //root node can be split
    TreePtr potential_split_node_ptr = root_node->insert_key(key, record_ptr);
    if(!is_null(potential_split_node_ptr)){
        //if split occurs at root, new root node should be created
        auto new_root = TreeNode::tree_node_factory(this->root_ptr, potential_split_node_ptr);
        this->root_ptr = new_root->tree_ptr;
        delete new_root;
    }
    delete root_node;
}

//deletes key from bptree if exists
void BPTree::delete_key(const Key &key) {
    TreeNode* root_node = TreeNode::tree_node_factory(this->root_ptr);
    root_node->delete_key(key);
    //height of bptree should be decreased
    if(root_node->size == 1 && root_node->node_type == INTERNAL) {
        this->root_ptr = root_node->single_child_ptr();
        root_node->delete_node();
    }
    delete root_node;
}

//run RANGE query using bptree
void BPTree::range(ostream &os, const Key &min_key, const Key &max_key) const {
    TreeNode* root_node = TreeNode::tree_node_factory(this->root_ptr);
    root_node->range(os, min_key, max_key);
    delete root_node;
}

//exports bptree. Used for grading.
void BPTree::export_bptree() const {
    ofstream fout(DATA_PATH + "bptree.md");
    auto root_node = TreeNode::tree_node_factory(this->root_ptr);
    root_node->export_node(fout);
    delete root_node;
    fout.close();
}

//creates mermaid chart of bptree
void BPTree::chart(ostream &os) const {
    os << "```mermaid" << endl;
    os << "graph TD" << endl;
    os << "subgraph B+TREE" << endl;
    auto root_node = TreeNode::tree_node_factory(this->root_ptr);
    root_node->chart(os);
    delete root_node;
    os << "end" << endl;
    os << "```" << endl;
}