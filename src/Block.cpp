#include "Block.hpp"
#include "RecordPtr.hpp"

//Function initializes block pointed to by block_ptr else creates a new block
Block::Block(const BlockPtr& block_ptr){
    this->block_ptr = block_ptr;
    this->size = 0;
    this->keys.assign(BLOCK_SIZE, -1);
    this->next_block_ptr = NULL_PTR;
    if(!is_null(this->block_ptr)){
        ifstream fin(TEMP_PATH + block_ptr);
        fin >> this;
        fin.close();
    } else {
        this->block_ptr = gen_random_file_name();
        this->dump();
    }
}

//returns TRUE if block is full
bool Block::is_full() {
    if(this->size < BLOCK_SIZE)
        return false;
    return true;
}

//returns true if block is empty
bool Block::is_empty() const {
    if(this->size == 0)
        return true;
    return false;
}

//function inserts record into block if space exists
//NOTE: This function doesn't check if space exists
RecordPtr Block::insert_key(const Key& key) {
    RecordPosition record_pos = 0;
    for(; record_pos < BLOCK_SIZE; record_pos++){
        if(this->keys[record_pos] == DELETE_MARKER){
            this->keys[record_pos] = key;
            this->size++;
            this->dump();
            break;
        }
    }
    RecordPtr record_ptr(this->block_ptr, record_pos);
    return record_ptr;
}

//adds a link from this block to block pointed by block_ptr
void Block::insert_next_block(BlockPtr block_ptr) {
    this->next_block_ptr = block_ptr;
    this->dump();
}

//deletes remove_key if exists
//returns TRUE if remove_key existed and was deleted, FALSE otherwise
bool Block::delete_key(const Key& remove_key) {
    bool removed = false;
    for(int i = 0; i < BLOCK_SIZE; i++)
        if(this->keys[i] == remove_key){
            this->keys[i] = DELETE_MARKER;
            removed = true;
            this->size--;
            this->dump();
            break;
        }
    return removed;
}

//deletes this block on disk
void Block::delete_block() {
    delete_file(TEMP_PATH + this->block_ptr);
    this->block_ptr = NULL_PTR;
    this->size = 0;
    this->keys.clear();
    this->next_block_ptr = NULL_PTR;
}

//exports block - used for grading. Do not change.
void Block::export_block(ostream& os) {
    os << this->size << endl;
    for(int i = 0; i < BLOCK_SIZE; i++)
        if(this->keys[i] != DELETE_MARKER)
            os << this->keys[i] << " ";
    os << endl;
}

void Block::range(ostream &os, const Key &min_key, const Key &max_key) {
    BLOCK_ACCESSES++;
    for(int i = 0; i < BLOCK_SIZE; i++){
        if(this->keys[i] != DELETE_MARKER && this->keys[i] >= min_key && this->keys[i] <= max_key)
            os << this->keys[i] << endl;
    }
}

//creates a node in the mermaid chart
void Block::chart(ostream &os) const {
    string chart_node = this->block_ptr + "[" + this->block_ptr + "<br/>";
    chart_node += "size: " + to_string(this->size) + "<br/>";
    for(const auto& key: this->keys)
        if(key == DELETE_MARKER)
            chart_node += "_ ";
        else
            chart_node += to_string(key) + " ";
    chart_node += "]";
    os << chart_node << endl;
}

//Function writes block from MM to disk
void Block::dump(){
    ofstream fout(TEMP_PATH + this->block_ptr);
    fout << this;
    fout.close();
}

bool Block::operator==(const Block* other) const {
    if(this->block_ptr == other->block_ptr)
        return true;
    return false;
}

bool Block::operator==(BlockPtr block_ptr) const {
    if(this->block_ptr == block_ptr)
        return true;
    return false;
}

Key Block::operator[](int index) const {
    if(index >= BLOCK_SIZE)
        return -1;
    return this->keys[index];
}

ostream& operator<<(ostream& os, Block* block){
    if(&os == &cout){
        cout << "block id: " << block->block_ptr << endl;
        cout << "num records: " << block->size << endl;
    } else {
        os << block->block_ptr << endl;
        os << block->size << endl;
    }
    for (int i = 0; i < BLOCK_SIZE; i++){
        if(&os == &cout && block->keys[i] == DELETE_MARKER)
            os << "_" << " ";
        else
            os << block->keys[i] << " ";
    }
    os << endl;
    if(&os == &cout)
        cout << "next block: " << block->next_block_ptr;
    else
        os << block->next_block_ptr;

    return os;
}

istream& operator>>(istream& is, Block* block){
    if(&is == &cin){
        cout << "block id: ";
        cin >> block->block_ptr;
        cout << "num records: ";
        cin >> block->size;
    } else {
        is >> block->block_ptr;
        is >> block->size;
    }
    for (int i = 0; i < BLOCK_SIZE; i++){
        is >> block->keys[i];
    }
    if(&is == &cin)
        cout << "next block: ";
    is >> block->next_block_ptr;
    return is;
}