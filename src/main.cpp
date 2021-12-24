#include "RecordPtr.hpp"
#include "UnorderedHeap.hpp"
#include "BPTree.hpp"
#include "Query.hpp"

const uint BLOCK_SIZE = 4; // >= 2 (in number of records/keys)
const uint FANOUT = 3;     //fanout >= 3
const uint MIN_OCCUPANCY = ceil((float)FANOUT / 2.0);

uint BLOCK_ACCESSES = 0;

const int DELETE_MARKER = -1; // data is non-negative integers
const string NULL_PTR = "-";  // used as null pointers for BlockPtr and TreePtr
const string BREAK = "<br/>"; //used to create chart
const string DATA_PATH = "../data/";
const string TEMP_PATH = "../temp/";

UnorderedHeap heap;
BPTree bptree;

int main(int argc, char *argv[])
{
    initialize();
    if (argc > 1)
        Query::execute_script(argv[1]); //executes all commands in data/argv[1]
    else
        Query::command_line_interpreter(); //initializes command line interpreter
    clear_temp_folder();
    return 0;
}
