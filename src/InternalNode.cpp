#include "InternalNode.hpp"

//creates internal node pointed to by tree_ptr or creates a new one
InternalNode::InternalNode(const TreePtr &tree_ptr) : TreeNode(INTERNAL, tree_ptr) {
    this->keys.clear();
    this->tree_pointers.clear();
    if(!is_null(tree_ptr))
        this->load();
}

//max element from tree rooted at this node
Key InternalNode::max() {
    Key max_key = DELETE_MARKER;
    TreeNode* last_tree_node = TreeNode::tree_node_factory(this->tree_pointers[this->size - 1]);
    max_key = last_tree_node->max();
    delete last_tree_node;
    return max_key;
}

//if internal node contains a single child, it is returned
TreePtr InternalNode::single_child_ptr() {
    if(this->size == 1)
        return this->tree_pointers[0];
    return NULL_PTR;
}

//inserts <key, record_ptr> into subtree rooted at this node.
//returns pointer to split node if exists
TreePtr InternalNode::insert_key(const Key &key, const RecordPtr &record_ptr) {
    int pos = 0;
    while(pos < this->size - 1 && this->keys[pos] != DELETE_MARKER && key > this->keys[pos])
        pos++;

    auto child_node = TreeNode::tree_node_factory(this->tree_pointers[pos]);
    TreePtr potential_split_child_node_ptr = child_node->insert_key(key, record_ptr);

    if(is_null(potential_split_child_node_ptr)) {
        delete child_node;
        return NULL_PTR;
    }

    // shifting tree pointers to the right by 1 starting from pos + 1 to create space for split child ptr,
    // adding the split_child_node_ptr at (pos + 1)
    this->size++;
    this->tree_pointers.push_back(NULL_PTR);
    for(int i = this->size - 1; i >= pos + 2; i--)
        this->tree_pointers[i] = this->tree_pointers[i - 1];
    this->tree_pointers[pos + 1] = potential_split_child_node_ptr;
    this->keys.push_back(DELETE_MARKER);
    for(int i = this->size - 2; i >= pos + 1; i--)
        this->keys[i] = this->keys[i - 1];
    this->keys[pos] = child_node->max();
    delete child_node;

    // no overflow after adding new child ptr
    if(!this->overflows()) {
        this->dump();
        return NULL_PTR;
    }

    // Overflow occured, creating new internal node
    InternalNode* new_tree_node = dynamic_cast<InternalNode*>(TreeNode::tree_node_factory(INTERNAL));
    TreePtr new_tree_ptr = new_tree_node->tree_ptr;

    // MIN_OCCUPANCY tree_pointers will remain in current internal node,
    // rest will go to the new internal node
    for(int i = MIN_OCCUPANCY; i < this->size; i++) {
        new_tree_node->tree_pointers.push_back(this->tree_pointers[i]);
        new_tree_node->size++;
    }
    this->tree_pointers.resize(MIN_OCCUPANCY);
    for(int i = MIN_OCCUPANCY; i < this->size - 1; i++) {
        new_tree_node->keys.push_back(this->keys[i]);
    }
    this->keys.resize(MIN_OCCUPANCY - 1);
    this->size = MIN_OCCUPANCY;

    new_tree_node->dump();
    this->dump();
    delete new_tree_node;
    return new_tree_ptr;
}

// last tree pointer is moved from the internal node to its right sibling node
void InternalNode::left_redistribute(TreeNode* right_sibling_tree_node) {
    InternalNode* right_sibling_node = dynamic_cast<InternalNode*>(right_sibling_tree_node);
    right_sibling_node->keys.push_back(DELETE_MARKER);
    right_sibling_node->tree_pointers.push_back(NULL_PTR);
    right_sibling_node->size++;
    for(int i = right_sibling_node->size - 1; i >= 1; i--) {
        right_sibling_node->tree_pointers[i] = right_sibling_node->tree_pointers[i - 1];
    }
    for(int i = right_sibling_node->size - 2; i >= 1; i--) {
        right_sibling_node->keys[i] = right_sibling_node->keys[i - 1];
    }
    right_sibling_node->tree_pointers[0] = this->tree_pointers[this->size - 1];
    auto right_sibling_first_child_node = TreeNode::tree_node_factory(right_sibling_node->tree_pointers[0]);
    right_sibling_node->keys[0] = right_sibling_first_child_node->max();
    delete right_sibling_first_child_node;

    this->size--;
    this->keys.pop_back();
    this->tree_pointers.pop_back();
    
    this->dump();
    right_sibling_node->dump();
}

// first tree pointer is moved from the internal node to its left sibling node
void InternalNode::right_redistribute(TreeNode* left_sibling_tree_node) {
    InternalNode* left_sibling_node = dynamic_cast<InternalNode*>(left_sibling_tree_node);
    auto left_sibling_last_child_node = TreeNode::tree_node_factory(left_sibling_node->tree_pointers.back());
    left_sibling_node->keys.push_back(left_sibling_last_child_node->max());
    left_sibling_node->tree_pointers.push_back(this->tree_pointers[0]);
    delete left_sibling_last_child_node;
    left_sibling_node->size++;

    this->size--;
    for(int i = 0; i < this->size; i++) {
        this->tree_pointers[i] = this->tree_pointers[i + 1];
    }
    for(int i = 0; i < this->size - 1; i++) {
        this->keys[i] = this->keys[i + 1];
    }
    this->keys.pop_back();
    this->tree_pointers.pop_back();

    this->dump();
    left_sibling_node->dump();
}

// merge sibling node with itself and delete sibling node
void InternalNode::merge_node(TreeNode* sibling_tree_node) {
    InternalNode* sibling_node = dynamic_cast<InternalNode*>(sibling_tree_node);
    auto self_last_child_node = TreeNode::tree_node_factory(this->tree_pointers.back());
    this->keys.push_back(self_last_child_node->max());
    delete self_last_child_node;

    for(int i = 0; i < sibling_node->size; i++) {
        this->tree_pointers.push_back(sibling_node->tree_pointers[i]);
    }
    for(int i = 0; i < sibling_node->size - 1; i++) {
        this->keys.push_back(sibling_node->keys[i]);
    }
    this->size += sibling_node->size;

    sibling_node->delete_node();
    this->dump();
}

//deletes key from subtree rooted at this if exists
void InternalNode::delete_key(const Key &key) {
    int pos = 0;
    while(pos < this->size - 1 && this->keys[pos] != DELETE_MARKER && key > this->keys[pos])
        pos++;

    auto child_node = TreeNode::tree_node_factory(this->tree_pointers[pos]);
    child_node->delete_key(key);
    if(pos < this->size - 1)
        this->keys[pos] = child_node->max();

    if(!child_node->underflows()) {
        delete child_node;
        this->dump();
        return;
    }

    if(pos > 0) {
        auto left_sibling_node = TreeNode::tree_node_factory(this->tree_pointers[pos - 1]);
        if(left_sibling_node->size + child_node->size >= 2 * MIN_OCCUPANCY) {
            left_sibling_node->left_redistribute(child_node);
            this->keys[pos - 1] = left_sibling_node->max();
        }
        else {
            left_sibling_node->merge_node(child_node);
            for(int i = pos; i < this->size - 1; i++) {
                this->tree_pointers[i] = this->tree_pointers[i + 1];
            }
            for(int i = pos - 1; i < this->size - 2; i++) {
                this->keys[i] = this->keys[i + 1];
            }
            this->tree_pointers.pop_back();
            this->keys.pop_back();
            this->size--;
        }
        delete left_sibling_node;
    }
    else {
        auto right_sibling_node = TreeNode::tree_node_factory(this->tree_pointers[pos + 1]);
        if(right_sibling_node->size + child_node->size >= 2 * MIN_OCCUPANCY) {
            right_sibling_node->right_redistribute(child_node);
            this->keys[pos] = child_node->max();
        }
        else {
            child_node->merge_node(right_sibling_node);
            for(int i = pos + 1; i < this->size - 1; i++) {
                this->tree_pointers[i] = this->tree_pointers[i + 1];
            }
            for(int i = pos; i < this->size - 2; i++) {
                this->keys[i] = this->keys[i + 1];
            }
            this->tree_pointers.pop_back();
            this->keys.pop_back();
            this->size--;
        }
        delete right_sibling_node;
    }
    delete child_node;
    this->dump();
}

//runs range query on subtree rooted at this node
void InternalNode::range(ostream &os, const Key &min_key, const Key &max_key) const {
    BLOCK_ACCESSES++;
    for(int i = 0; i < this->size - 1; i++){
        if(min_key <= this->keys[i]){
            auto* child_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
            child_node->range(os, min_key, max_key);
            delete child_node;
            return;
        }
    }
    auto* child_node = TreeNode::tree_node_factory(this->tree_pointers[this->size - 1]);
    child_node->range(os, min_key, max_key);
    delete child_node;
}

//exports node - used for grading
void InternalNode::export_node(ostream &os) {
    TreeNode::export_node(os);
    for(int i = 0; i < this->size - 1; i++)
        os << this->keys[i] << " ";
    os << endl;
    for(int i = 0; i < this->size; i++){
        auto child_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
        child_node->export_node(os);
        delete child_node;
    }
}

//writes subtree rooted at this node as a mermaid chart
void InternalNode::chart(ostream &os) {
    string chart_node = this->tree_ptr + "[" + this->tree_ptr + BREAK;
    chart_node += "size: " + to_string(this->size) + BREAK;
    chart_node += "]";
    os << chart_node << endl;

    for(int i = 0; i < this->size; i++) {
        auto tree_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
        tree_node->chart(os);
        delete tree_node;
        string link = this->tree_ptr + "-->|";

        if(i == 0)
            link += "x <= " + to_string(this->keys[i]);
        else if (i == this->size - 1) {
            link += to_string(this->keys[i-1]) + " < x";
        } else {
            link += to_string(this->keys[i-1]) + " < x <= " + to_string(this->keys[i]);
        }
        link += "|" + this->tree_pointers[i];
        os << link << endl;
    }
}

ostream& InternalNode::write(ostream &os) const {
    TreeNode::write(os);
    for(int i = 0; i < this->size - 1; i++){
        if(&os == &cout)
            os << "\nP" << i+1 << ": ";
        os << this->tree_pointers[i] << " ";
        if(&os == &cout)
            os << "\nK" << i+1 << ": ";
        os << this->keys[i] << " ";
    }
    if(&os == &cout)
        os << "\nP" << this->size << ": ";
    os << this->tree_pointers[this->size - 1];
    return os;
}

istream& InternalNode::read(istream& is){
    TreeNode::read(is);
    this->keys.assign(this->size - 1, DELETE_MARKER);
    this->tree_pointers.assign(this->size, NULL_PTR);
    for(int i = 0; i < this->size - 1; i++){
        if(&is == &cin)
            cout << "P" << i+1 << ": ";
        is >> this->tree_pointers[i];
        if(&is == &cin)
            cout << "K" << i+1 << ": ";
        is >> this->keys[i];
    }
    if(&is == &cin)
        cout << "P" << this->size;
    is >> this->tree_pointers[this->size - 1];
    return is;
}
