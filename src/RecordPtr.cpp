#include "RecordPtr.hpp"

RecordPtr::RecordPtr(BlockPtr block_ptr, RecordPosition record_pos) {
    this->block_ptr = block_ptr;
    this->record_pos = record_pos;
}

void RecordPtr::write_data(ostream &os) const {
    BLOCK_ACCESSES++;
    if(is_null(this->block_ptr))
        return;
    auto block = new Block(this->block_ptr);
    os << (*block)[this->record_pos] << endl;
    delete block;
}

ostream& operator<<(ostream& os, const RecordPtr& record_ptr) {
    if(&os == &cout)
        os << "(" << record_ptr.block_ptr << ", " << record_ptr.record_pos << ")";
    else
        os << record_ptr.block_ptr << " " << record_ptr.record_pos;
    return os;
}

istream& operator>>(istream& is, RecordPtr& record_ptr){
    if(&is == &cin)
        cout << "block id: ";
    is >> record_ptr.block_ptr;
    if(&is == &cin)
        cout << "record position: ";
    is >> record_ptr.record_pos;
    return is;
}