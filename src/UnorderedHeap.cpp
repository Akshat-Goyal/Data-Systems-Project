#include "UnorderedHeap.hpp"
#include "Block.hpp"
#include "RecordPtr.hpp"

UnorderedHeap::UnorderedHeap(BlockPtr block_ptr) {
    Block* new_block = new Block(block_ptr);
    this->first_block_ptr = new_block->block_ptr;
    delete new_block;
}

RecordPtr UnorderedHeap::insert_key(const Key& key) const {
    BlockPtr current_block_ptr = this->first_block_ptr;
    BlockPtr last_block_ptr = NULL_PTR;
    bool inserted = false;
    RecordPtr record_ptr;

    //find appropriate block to put record in if exists
    while(!is_null(current_block_ptr)){
        auto block = new Block(current_block_ptr);
        //if there is space in the block
        if(!block->is_full()){
            record_ptr = block->insert_key(key);
            inserted = true;
            delete block;
            break;
        }
        if(is_null(block->next_block_ptr))
            last_block_ptr = current_block_ptr;
        current_block_ptr = block->next_block_ptr;
        delete block;
    }

    //All blocks are full
    if(!inserted){
        auto new_block = new Block(); //creating new block
        record_ptr = new_block->insert_key(key);
        auto last_block = new Block(last_block_ptr); //adding pointer from last block to new block
        last_block->insert_next_block(new_block->block_ptr);
        delete new_block;
        delete last_block;
    }
    return record_ptr;
}

void UnorderedHeap::delete_key(const Key& key) {
    BlockPtr current_block_ptr = this->first_block_ptr;
    BlockPtr previous_block_ptr = NULL_PTR;
    BlockPtr next_block_ptr = NULL_PTR;
    bool removed = false;

    //iterate through the blocks in the heap
    while(!is_null(current_block_ptr)){
        auto* block = new Block(current_block_ptr);
        removed = block->delete_key(key);
        next_block_ptr = block->next_block_ptr;
        delete block;
        //data is a key, only one record exists
        if(removed)
            break;
        previous_block_ptr = current_block_ptr;
        current_block_ptr = next_block_ptr;
    }

    //no deletion took place
    if(!removed)
        return;

    auto* current_block = new Block(current_block_ptr);
    //current block is empty should be removed
    if(current_block->is_empty()){
        //If current block is the front block
        if(is_null(previous_block_ptr)){
            //the current block is the only block in the heap
            if(is_null(next_block_ptr))
                this->first_block_ptr = (new Block())->block_ptr;
            else
                this->first_block_ptr = next_block_ptr;
        }
        //current block is not front block
        else {
            auto* previous_block = new Block(previous_block_ptr);
            previous_block->insert_next_block(next_block_ptr);
            delete previous_block;
        }
        current_block->delete_block();
    }
    delete current_block;
}

void UnorderedHeap::export_heap() const {
    ofstream fout(DATA_PATH + "heap.md");
    BlockPtr current_block_ptr = this->first_block_ptr;
    while(!is_null(current_block_ptr)){
        auto block = new Block(current_block_ptr);
        block->export_block(fout);
        current_block_ptr = block->next_block_ptr;
        delete block;
    }
    fout.close();
}

void UnorderedHeap::range(ostream &os, const Key &min_key, const Key &max_key) const {
    BlockPtr current_block_ptr = this->first_block_ptr;
    while(!is_null(current_block_ptr)){
        auto block = new Block(current_block_ptr);
        block->range(os, min_key, max_key);
        current_block_ptr = block->next_block_ptr;
        delete block;
    }
}

void UnorderedHeap::chart(ostream& os) const{
    os << "```mermaid" << endl;
    os << "graph LR" << endl;
    os << "subgraph UNORDERED_HEAP" << endl;
    BlockPtr current_block_ptr = this->first_block_ptr;
    BlockPtr previous_block_ptr = NULL_PTR;
    while(!is_null(current_block_ptr)){
        auto block = new Block(current_block_ptr);
        block->chart(os);
        if(!is_null(previous_block_ptr))
            os << previous_block_ptr << "-->" << current_block_ptr << endl;
        previous_block_ptr = current_block_ptr;
        current_block_ptr = block->next_block_ptr;
        delete block;
    }
    os << "end" << endl;
    os << "```" << endl;
}

ostream& operator<<(ostream& os, UnorderedHeap unordered_heap) {
    BlockPtr current_block_ptr = unordered_heap.first_block_ptr;
    int block_id = 1;
    os << "UNORDERED HEAP" << endl;
    while(!is_null(current_block_ptr)){
        auto *block = new Block(current_block_ptr);
        os << "\nBLOCK " << block_id << endl;
        os << block << endl;
        current_block_ptr = block->next_block_ptr;
        delete block;
        block_id++;
    }
    return os;
}