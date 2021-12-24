# Data Systems

This is a simplified relational database management system which supports basic database operations on integer-only tables such as load, store, etc and transpose operations efficiently on arbitrarily large dense and sparse matrices. There is no support for transaction or thread safety.

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

## Branch Info

- ```master``` branch supports transposing matrices.

- ```bptree``` branch supports B+ tree indices.

- ```join-and-group-by``` branch supports join and group by commands.

- ```sort``` branch supports two-phase sort algorithm.

## To run

Post compilation, an executable names ```server``` will be created in the ```src``` directory
```
./server
```

## Commands / Queries

- Look at the [Overview.html](./docs/Overview.md) to understand the syntax and working of the table related queries.

- ```LOAD MATRIX <matrix_name>```:
The LOAD MATRIX command loads contents of the .csv (stored in ```data``` folder) and stores it as blocks in the ```data/temp``` directory.

- ```PRINT MATRIX <matrix_name>```:
PRINT MATRIX command prints the first 20 rows of the matrix on the terminal.

- ```TRANSPOSE <matrix_name>```:
TRANSPOSE command transposes the matrix IN PLACE (without using any additional disk blocks) and writes it back into the same blocks the matrix was stored in.

- ```EXPORT MATRIX <matrix_name>```:
EXPORT command writes the contents of the matrix named
<matrix_name> into a file called <matrix_name>.csv in ```data``` folder.
