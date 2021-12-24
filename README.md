# Data Systems

This is a simplified relational database management system which supports basic database operations on integer-only tables such as load, store, etc, and sort.

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

- ```Sort```
```
<new_table_name> <- SORT <table_name> BY <column_name> IN ASC | DESC BUFFER <buffer_size>
```
Two-Phase Merge Sort algorithm is used to implement sort command.