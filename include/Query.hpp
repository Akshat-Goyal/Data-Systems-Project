/**
 * Query class provides a query engine that facilitates execution of predefined commands.
 *
 * Accepted commands are - INSERT, DELETE, RANGE, SOURCE, EXPORT and QUIT.
 * syntax:
 *          INSERT <insert_key>       : ex INSERT 2 - inserts 2 into bptree and heap
 *          DELETE <delete_key>       : ex DELETE 2 - deletes 2 from bptree and heap if they exist
 *          RANGE <min_key> <max_key> : ex RANGE 2 4 - retrieves set of keys between
 *                                      min_key and max_key inclusive. Result is written into
 *                                      file called "range_[<min_key>_<max_key>].txt"
 *                                      ex range_[2_4].txt
 *          EXPORT                    : ex EXPORT - exports both bptree and heap. Used for grading
 *          SOURCE <file_name>        : ex SOURCE demo - runs all commands in the data/demo file
 *          QUIT                      : ex QUIT - used to quit the command line interpreter
 *
 * Any misspelled command is interpreted as QUIT
 *
 * Do not change this class.
 *
 */

#ifndef BPTREE_QUERY_HPP
#define BPTREE_QUERY_HPP

#include "BPTree.hpp"

enum QueryType {
    INSERT,
    DELETE,
    RANGE,
    EXPORT,
    SOURCE,
    QUIT
};

istream& operator>>(istream& is, QueryType& query_type);

class Query {
public:
    QueryType query_type;
    Key insert_key; //insert query
    Key delete_key; //delete query
    Key min_range_key; //range query
    Key max_range_key;
    string source_file_name;

    Query();
    explicit Query(const QueryType& query_type, const Key& key1 = DELETE_MARKER,
                   const Key& key2 = DELETE_MARKER, string file_name = "");

    void execute() const;
    void execute_insert() const;
    void execute_delete() const;
    void execute_range() const;
    static void execute_export();
    void execute_source() const;
    static void chart();

    static void execute_script(string script_name = "demo");
    static void command_line_interpreter();

    friend istream& operator>>(istream& is, Query* query);
};


#endif //BPTREE_QUERY_HPP
