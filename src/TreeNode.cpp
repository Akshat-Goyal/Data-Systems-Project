#include "TreeNode.hpp"
#include "InternalNode.hpp"
#include "LeafNode.hpp"

ostream& operator<<(ostream& os, const NodeType& node_type){
    if(&os == &cout)
        os << "node type: ";
    switch (node_type) {
        case LEAF: os << "LEAF"; break;
        case INTERNAL: os << "INTERNAL"; break;
        default: cout << "unrecognized node type" << endl;
    }
    return os;
}

istream& operator>>(istream& is, NodeType& node_type){
    if(&is == &cin)
        cout << "node type: ";
    string node_type_str;
    is >> node_type_str;
    if(node_type_str == "LEAF")
        node_type = LEAF;
    else if(node_type_str == "INTERNAL")
        node_type = INTERNAL;
    return is;
}

TreeNode::TreeNode(const NodeType& node_type, const TreePtr &tree_ptr) {
    this->node_type = node_type;
    this->tree_ptr = tree_ptr;
    this->size = 0;
    if(is_null(this->tree_ptr)){
        this->tree_ptr = gen_random_file_name();
        this->dump();
    }
}

//creates new node of type node_type
TreeNode* TreeNode::tree_node_factory(NodeType node_type) {
    TreeNode* new_tree_node;
    switch (node_type) {
        case LEAF: new_tree_node = new LeafNode(); break;
        case INTERNAL: new_tree_node = new InternalNode(); break;
    }
    return new_tree_node;
}

//creates a node pointer loading node from tree_ptr
TreeNode* TreeNode::tree_node_factory(TreePtr tree_ptr) {
    NodeType node_type = LEAF;
    ifstream fin(TEMP_PATH + tree_ptr);
    fin >> node_type;
    fin.close();

    switch (node_type) {
        case LEAF: return new LeafNode(tree_ptr);
        case INTERNAL: return new InternalNode(tree_ptr);
    }
}

//creates a new internal node with two child nodes - child_tree_ptr1 & child_tree_ptr2
TreeNode* TreeNode::tree_node_factory(const TreePtr &child_tree_ptr1, const TreePtr &child_tree_ptr2) {
    auto new_internal_node = new InternalNode();
    new_internal_node->tree_pointers.push_back(child_tree_ptr1);
    new_internal_node->tree_pointers.push_back(child_tree_ptr2);
    auto left_child_node = TreeNode::tree_node_factory(child_tree_ptr1);
    new_internal_node->keys.push_back(left_child_node->max());
    delete left_child_node;
    new_internal_node->size = 2;
    new_internal_node->dump();
    return new_internal_node;
}

bool TreeNode::is_empty() const{
    if(this->size == 0)
        return true;
    return false;
}

bool TreeNode::is_full() const {
    if(this->size == FANOUT)
        return true;
    return false;
}

bool TreeNode::overflows() const {
    if(this->size > FANOUT)
        return true;
    return false;
}

bool TreeNode::underflows() const {
    if(this->size < MIN_OCCUPANCY)
        return true;
    return false;
}

void TreeNode::export_node(ostream& os){
    os << this->node_type << endl;
    os << this->size << endl;
}

void TreeNode::dump() const{
    ofstream fout(TEMP_PATH + this->tree_ptr);
    this->write(fout);
    fout.close();
}

void TreeNode::load() {
    ifstream fin(TEMP_PATH + this->tree_ptr);
    this->read(fin);
    fin.close();
}

void TreeNode::delete_node() const {
    delete_file(TEMP_PATH + this->tree_ptr);
}

ostream& TreeNode::write(ostream& os) const {
    if(&os == &cout){
        os << "node type: " << this->node_type << endl;
        os << "tree node id: " << this->tree_ptr << endl;
        os << "size: " << this->size << endl;
    } else {
        os << this->node_type << endl;
        os << this->tree_ptr << endl;
        os << this->size << endl;
    }
    return os;
}

istream& TreeNode::read(istream& is){
    if(&is == &cin){
        cout << "node type: "; cin >> this->node_type;
        cout << "tree node id: "; cin >> this->tree_ptr;
        cout << "size: "; cin >> this->size;
    } else {
        is >> this->node_type;
        is >> this->tree_ptr;
        is >> this->size;
    }
    return is;
}

ostream& operator<<(ostream& os, TreeNode* tree_node){
    tree_node->write(os);
    return os;
}

istream& operator>>(istream& is, TreeNode* tree_node){
    tree_node->read(is);
    return is;
}


