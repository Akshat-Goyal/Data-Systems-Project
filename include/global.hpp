#ifndef BPTREE_GLOBAL_HPP
#define BPTREE_GLOBAL_HPP

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <ctime>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <cctype>
#include <unistd.h>
#include <cmath>

using namespace std;

typedef unsigned int uint;

extern const uint BLOCK_SIZE;	 //in records i.e. block can store 10 records
extern const uint FANOUT;		 //fanout of b+tree
extern const uint MIN_OCCUPANCY; //minimum number of children in each node

extern const string DATA_PATH;
extern const string TEMP_PATH;
extern const int DELETE_MARKER;
extern const string NULL_PTR;
extern const string BREAK;

extern uint BLOCK_ACCESSES;

class Block;
class RecordPtr;
class UnorderedHeap;
class TreeNode;
class InternalNode;
class LeafNode;
class BPTree;

string gen_random_file_name();
void initialize();
void clear_temp_folder();
void delete_file(string file_name);
bool is_null(string pointer);

extern UnorderedHeap heap;
extern BPTree bptree;
#endif //BPTREE_GLOBAL_HPP
