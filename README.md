# Data Systems

This is a simplified relational database management system which supports basic database operations on integer-only tables such as load, store, etc, and two types of join and group by also.

## Compilation Instructions

We use ```make``` to compile all the files and create the server executable. ```make``` is used primarily in Linux systems, so those of you who want to use Windows will probably have to look up alternatives (I hear there are ways to install ```make``` on Windows). To compile

```cd``` into the directory
```cd``` into the soure directory (called ```src```)
```
cd src
```
To compile
```
make clean
make
```

## To run

Post compilation, an executable names ```server``` will be created in the ```src``` directory
```
./server
```

## Commands / Queries

- Look at the [Overview.html](./docs/Overview.md) to understand the syntax and working of the table related queries.

- ```Block Nested Join```:
```
<new_relation_name> <- JOIN USING NESTED <table1>,  <table2> ON <column1> <binary_operation> <column2> BUFFER <buffer_size>
```

- ```Partition Hash Join```:
```
<new_relation_name> <- JOIN USING PARTHASH <table1>, <table2> ON <column1> <binary_operation> <column2> BUFFER <buffer_size>
```

- ```Group By```
```
<new_table> <- GROUP BY <grouping_attribute> FROM <table_name> RETURN MAX|MIN|SUM|AVG(<attribute>)
```
