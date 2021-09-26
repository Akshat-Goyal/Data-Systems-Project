# Extending SimpleRA to include Matrices

### Team 10 - Akshat Goyal (2018101075) and Tanish Lad (2018114005)

## Normal (Dense) Matrices

## Page Layout

We split the `N` x `N` matrix into smaller `M` x `M` submatrices and each submatrix is written to a disk block.

`M = sqrt((BLOCK\_SIZE * 1024) / sizeof(int))` which comes out to be `45`.

We use this technique because this makes the transpose much more efficient. Storing in the row major order is very easy but transpose will take a lot of time, which is undesirable.

We also take care of the fact that `N` may not be divisible by `M`. But that is just an implementation detail.

Rest of the details about how we use this layout is mentioned below in the operations section.

---

## Operations
### LOAD MATRIX

In short, we are reading the csv file only once but writing in each page multiple times.

We do the following:
1. Read the first `M` elements of the file and write it to the first page.
2. Read the next `M` elements of the file and write it to the next page.
3. Continue this until we reach the end of the first row of the file. Then we move to the next row of the file and we start **appending** back to the first page.
4. After we write `M` rows in a page, then we skip approximately the next `N/M` pages and start writing to this new page.
5. We do this until we reach the end of the file. This will write the whole matrix in the form of smaller submatrices.

In practice, this works better than storing each submatrix completely and writing the whole submatrix all at once in a page. This writes in a page only once, but it reads the file multiple times. But it is worse than reading once and writing multiple times because appending to a file is fast.

### PRINT MATRIX and EXPORT MATRIX

Print and Export are very similar operations.

We do the following:
1. Write the first row of the first page.
2. Get to the next page using the cursor. Write the first row of the next page and move the cursor to the next page.
3. After we write the entire first row of the matrix, we move the cursor back to the first page, but instead to the second row instead of the fist row.
4. After `M` row writes, the cursor reaches to the end of a page, we move the cursor to the block which is immediate to its down i.e. skipping approximately `N / M` pages.
5. We do this until we reach the end of the last page. This will write the complete matrix.


### TRANSPOSE

Transpose is easy and fast.

For every disk block `(i, j)`, `i < j`:
1. Get it in memory
2. Load the disk block `(j, i)` in the memory.
3. Transpose both the matrices in `(i, j)` and `(j, i)` internally (inplace).
4. Write the page `(j, i)` where the block `(i, j)` was stored and write the page `(i, j)` where the block `(j, i)` was stored.

For every disk block `(i, i)`:
1. Get it in memory
2. Transpose it internally by swapping each element `(m, n)` with `(n, m)`
3. Write the page back to it's original location

This is an inplace operation because we are not using any additional disk blocks nor are we using any additional main memory.

### Implementation Details
The following changes were made to the code to implement the commands:

- The Matrix class was created, which resembles the table class in terms of functionality.
- The MatrixCatalogue class was created, which resembles the tableCatalogue class in terms of functionality
- The Page and BufferManager classes were modified, adding functions such as getMatrix(), writeMatrixPage(), getMatrixPage(), and deleteFile()
- The syntacticParser, semanticParser, and executor classes were modified to support the new commands
- Four new executors were created: loadMatrix, exportMatrix, printMatrix and transpose.

P.S. By default, we are not writing in the log. We have a set a flag that is off by default, in the `logger.h` file. You can turn it on if you want to see the log.

---

## Sparse Matrices
## Compression Technique and Page Layout

Using the normal representation will waste a huge amount of space because most elements in the matrix are zeros.

We also cannot compress the individual submatrices that we stored previously, because, although they will reduce the total size used, but the number of disk blocks/pages used will still be the same (or even more!). Hence we need to find an alternate representation for storing sparse matrices.

So, we store the non-zero elements in the form of a table. So, for every non-zero element, we add a row in the table. The row will be a tuple containing `3` elements: the row number, the column number, and the value.

So the tuple will be (row_number, column_number, value).

---

## Operations on a Sparse Matrix
### LOAD MATRIX
LOAD MATRIX operation is easy. It is done in a very similar way to how a **table** was loaded.

### PRINT MATRIX and EXPORT MATRIX
Print and Export are similar operations.

Assume we have the tuples sorted in increasing order of the row number. If the row number is same, they are sorted in increasing order of the column number.

We have to export `N * N` elements, so we iterate through each index of the matrix and check if the the corresponding row number and column number exists at the current position of the cursor. If yes, then we get the value from the third element of the tuple and take the cursor to the next position. Else, the value is `0` and the cursor stays at the same position.

This will only work if the tuples are sorted and hence we make sure to sort them after anything changes (changes happen only in the transpose operation).

### TRANSPOSE
For TRANSPOSE, since we are adopting a different representation to store sparse matrices, the original method used in dense matrices will not work here.

So, we do the following steps:
1. Iterate through each page. For each tuple, swap the first and second elements of it i.e. we swap the row number and the column number of the tuple. Then, after we complete a page, we sort all the rows in the page in increasing order of row number and then column number.

This is an inplace operation because we are not using any extra pages.

2. Then, we iterate through every pair of pages and merge them so that the smaller elements are in the first page and the larger elements are in the second page. The order in which we iterate through every pair is:
```
for page1 in range(n_pages):
    for page2 in range(page + 1, n_pages):
        // Merge page 1 and page 2
        // Write them back
```

This is also an inplace operation because we are taking the rows of both the pages in a new vector in memory, sorting the combined vector, and writing approximately half of them back to each page. We are not using any additional disk blocks, and we are using a limited amount of main memory (equivalent to two disk blocks).

After completion, it can be proved that all the tuples are completely sorted (It is similar to selection sort).


---
## Compression Ratio

Let the matrix be of dimensions `N` x `N`.
Let `p` be the percentage of sparseness of the matrix.

Expected number of non-zero elements `NZ = (1 - p) * N * N`.

Number of bytes required to store non-zero elements `B_1 = 3 * NZ * 4 = 12 * (1 - p) * N * N`

Number of bytes required to store the matrix using the normal representation `B_2 = 4 * N * N`

The compression ratio will be the number of bytes required by normal representation divided by the number of bytes required for sparse representation.

Hence, the compression ratio `C = \frac{B_2}{B_1} = \frac{4 * N * N}{12 * (1 - p) * N * N}`
`C = \frac{1}{3 * (1 - p)}`

For example
1. If `p = 0.6`, then `C = 1/1.2 = 0.83`
2. If `p = 0.\overline{66}`, then `C = 1`
3. If `p = 0.9`, then `C = 1/0.3 = 3.33`
4. If `p = 0.99`, then `C = 1/0.03 = 33.33`

As can be seen, the higher the sparseness is, the greater is the compression ratio.

---
