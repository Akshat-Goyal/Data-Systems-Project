#include "global.h"

/**
 * @brief Construct a new Matrix:: Matrix object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param matrixName 
 */
Matrix::Matrix(string matrixName)
{
    logger.log("Matrix::Matrix");
    this->sourceFileName = "../data/" + matrixName + ".csv";
    this->matrixName = matrixName;
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates matrix
 * statistics.
 *
 * @return true if the matrx has been successfully loaded 
 * @return false if an error occurred 
 */
bool Matrix::load()
{
    logger.log("Matrix::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        return this->blockify();
    }
    fin.close();
    return false;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. 
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Matrix::blockify()
{
    logger.log("Matrix::blockify");
    if (!this->setStatistics())
        return false;

    vector<vector<int>> rows(this->maxRowsPerBlock);

    for (int block_j = 0, columnPointer = 0; block_j < this->blocksPerRow; block_j++)
    {
        int columnsInBlock = min(this->columnCount - columnPointer, this->maxRowsPerBlock);

        ifstream fin(this->sourceFileName, ios::in);
        int block_i = 0, rowCounter = 0;
        while (true)
        {
            rows[rowCounter] = this->readRowSegment(columnPointer, columnsInBlock, fin);
            if (!rows[rowCounter].size())
                break;
            rowCounter++;
            if (rowCounter == this->maxRowsPerBlock)
            {
                int blockNum = block_i * this->blocksPerRow + block_j;
                bufferManager.writeMatrixPage(this->matrixName, blockNum, rows, rowCounter);
                block_i++;
                this->dimPerBlockCount[blockNum] = {rowCounter, columnsInBlock};
                rowCounter = 0;
            }
        }

        if (rowCounter)
        {
            int blockNum = block_i * this->blocksPerRow + block_j;
            bufferManager.writeMatrixPage(this->matrixName, blockNum, rows, rowCounter);
            block_i++;
            this->dimPerBlockCount[blockNum] = {rowCounter, columnsInBlock};
            rowCounter = 0;
        }
        columnPointer += columnsInBlock;
        fin.close();
    }

    if (this->rowCount == 0)
        return false;
    return true;
}

/**
 * @brief This function reads the segment of the row pointed by fin.
 *
 * @return vector<int>
 */
vector<int> Matrix::readRowSegment(int columnPointer, int columnsInBlock, ifstream &fin)
{
    logger.log("Matrix::readRowSegment");
    vector<int> row;
    string line, word;
    if (!getline(fin, line))
        return row;
    stringstream s(line);
    int columnCounter = 0, columnEndIndex = columnPointer + columnsInBlock;
    while (columnCounter < columnPointer)
    {
        getline(s, word, ',');
        columnCounter++;
    }
    while (columnCounter++ < columnEndIndex)
    {
        if (getline(s, word, ','))
            row.push_back(stoi(word));
    }
    return row;
}

/**
 * @brief Function finds total no. of columns (N), max rows per block 
 * (M) and no. of blocks required per row (ceil(N / M)). NxN matrix is 
 * split into smaller MxM matrix which can fit in a block.
 *
 */
bool Matrix::setStatistics()
{
    logger.log("Matrix::setStatistics");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    if (!getline(fin, line))
        return false;
    this->rowCount++;
    stringstream s(line);
    while (getline(s, word, ','))
        this->columnCount++;
    while (getline(fin, line))
        this->rowCount++;
    fin.close();
    this->maxRowsPerBlock = (uint)sqrt((BLOCK_COUNT * 1024) / sizeof(int));
    this->blocksPerRow = this->columnCount / this->maxRowsPerBlock + (this->columnCount % this->maxRowsPerBlock != 0);
    this->blockCount = this->blocksPerRow * this->blocksPerRow;
    this->dimPerBlockCount.resize(this->blockCount);
    return true;
}

/**
 * @brief Transposes matrix. Swaps block_ij with block_ji for each i, j, and 
 * transposes smaller matrix in each block
 *
 */
void Matrix::transpose()
{
    for (int block_i = 0; block_i < this->blocksPerRow; block_i++)
    {
        for (int block_j = 0; block_j < this->blocksPerRow; block_j++)
        {
            if (block_i != block_j)
            {
                int block_ij = block_i * this->blocksPerRow + block_j, block_ji = block_j * this->blocksPerRow + block_i;
                MatrixPage* page_ij = bufferManager.getMatrixPage(this->matrixName, block_ij);
                MatrixPage* page_ji = bufferManager.getMatrixPage(this->matrixName, block_ji);
                swap(page_ij, page_ji);
                swap(this->dimPerBlockCount[block_ij], this->dimPerBlockCount[block_ji]);
                this->transposePage(page_ij, block_ij);
                this->transposePage(page_ji, block_ji);
            }
            else
            {
                int block_ij = block_i * this->blocksPerRow + block_j;
                MatrixPage* page_ij = bufferManager.getMatrixPage(this->matrixName, block_ij);
                this->transposePage(page_ij, block_ij);
            }
        }
    }
}

/**
 * @brief Transposes matrix of the given block
 *
 */
void Matrix::transposePage(MatrixPage *page, int blockIdx)
{
    page->transpose();
    swap(this->dimPerBlockCount[blockIdx].first, this->dimPerBlockCount[blockIdx].second);
    bufferManager.writeMatrixPage(this->matrixName, blockIdx, page->getRows(), this->dimPerBlockCount[blockIdx].first);
}


/**
 * @brief Function prints the first few rows of the matrix. If the matrix contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Matrix::print()
{
    logger.log("Matrix::print");
    uint count = min((long long)PRINT_COUNT, this->rowCount);

    CursorMatrix cursor = this->getCursor();
    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        for (int seg = 0; seg < this->blocksPerRow; seg++)
            this->writeRow(cursor.getNext(), cout, seg == 0, seg == this->blocksPerRow - 1);
    }
    printRowCount(this->rowCount);
}

/**
 * @brief This function moves cursor to next page if next page exists.
 *
 * @param cursor 
 */
void Matrix::getNextPage(CursorMatrix *cursor)
{
    logger.log("Matrix::getNextPage");

    if (cursor->pageIndex < this->blockCount - 1)
    {
        cursor->nextPage(cursor->pageIndex + 1);
    }
}

/**
 * @brief This function moves cursor to point to the next segment 
 * of the matrix's row or to the starting of the next row if exists.
 *
 * @param cursor 
 */
void Matrix::getNextPointer(CursorMatrix *cursor)
{
    logger.log("Matrix::getNextPointer");
    
    if ((cursor->pageIndex + 1) % this->blocksPerRow == 0)
    {
        if (cursor->pagePointer == this->dimPerBlockCount[cursor->pageIndex].first - 1)
        {
            if (cursor->pageIndex < this->blockCount - 1)
                cursor->nextPage(cursor->pageIndex + 1);
        }
        else
            cursor->nextPage(cursor->pageIndex - this->blocksPerRow + 1, cursor->pagePointer + 1);
    }
    else if (cursor->pageIndex < this->blockCount - 1)
        cursor->nextPage(cursor->pageIndex + 1, cursor->pagePointer);
}

/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Matrix::makePermanent()
{
    logger.log("Matrix::makePermanent");
    if (!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->matrixName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    CursorMatrix cursor = this->getCursor();
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int seg = 0; seg < this->blocksPerRow; seg++)
            this->writeRow(cursor.getNext(), fout, seg == 0, seg == this->blocksPerRow - 1);
    }

    // vector<vector<int>> rows(this->maxRowsPerBlock, vector<int>(this->columnCount));
    // for (int rowwiseBlockCounter = 0, blockCounter = 0; rowwiseBlockCounter < this->blocksPerRow; rowwiseBlockCounter++)
    // {
    //     int blockRowCount = this->dimPerBlockCount[blockCounter].first;
    //     for (int columnCounter = 0; columnCounter < this->columnCount;)
    //     {
    //         auto blockDim = this->dimPerBlockCount[blockCounter++];
    //         for (int blockRowCounter = 0; blockRowCounter < blockDim.first; blockRowCounter++)
    //         {
    //             auto row = cursor.getNext();
    //             for (int blockColCounter = 0; blockColCounter < blockDim.second; blockColCounter++)
    //             {
    //                 rows[blockRowCounter][columnCounter++] = row[blockColCounter];
    //             }
    //         }
    //     }

    //     for (int blockRowCounter = 0; blockRowCounter < blockRowCount; blockRowCounter)
    //     {
    //         this->writeRow(rows[blockRowCounter], fout);
    //     }
    // }
    fout.close();
}

/**
 * @brief Function to check if matrix is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Matrix::isPermanent()
{
    logger.log("Matrix::isPermanent");
    if (this->sourceFileName == "../data/" + this->matrixName + ".csv")
        return true;
    return false;
}

/**
 * @brief The unload function removes the matrix from the database by deleting
 * all temporary files created as part of this table
 *
 */
void Matrix::unload()
{
    logger.log("Matrix::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
        bufferManager.deleteFile(this->matrixName, pageCounter);
    if (!isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
}

/**
 * @brief Function that returns a cursor that reads rows from this matrix
 * 
 * @return CursorMatrix 
 */
CursorMatrix Matrix::getCursor()
{
    logger.log("Matrix::getCursor");
    CursorMatrix cursor(this->matrixName, 0);
    return cursor;
}
