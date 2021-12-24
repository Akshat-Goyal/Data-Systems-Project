#include "global.hpp"

//Function generates a random file name that is used to store a block
string gen_random_file_name(){
    static int BLOCK_ID = 1;
    int FILE_NAME_LENGTH  = 5;
    string file_name;
    file_name.reserve(TEMP_PATH.length() + FILE_NAME_LENGTH + 1);
    for(int i=0; i< FILE_NAME_LENGTH; i++){
        file_name += 'a' + rand()%26;
    }
    file_name += to_string(BLOCK_ID);
    BLOCK_ID++;
    return file_name;
}

//function clears the temp folder
//Function needs to be changed if on WINDOWS
void clear_temp_folder(){
    string a = "rm -r " + TEMP_PATH;
    system(a.c_str());
    a = "mkdir " + TEMP_PATH;
    system(a.c_str());
}

//function initializes setup - clears temp folder and initializes seed for random number generator
void initialize(){
    srand( (unsigned) time(nullptr) * getpid());
    clear_temp_folder();
}

void delete_file(string file_name){
    remove(file_name.c_str());
}

bool is_null(string pointer){
    if(pointer == NULL_PTR)
        return true;
    return false;
}
