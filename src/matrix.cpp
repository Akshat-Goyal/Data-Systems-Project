#include "global.h"

/**
 * @brief Construct a new Matrix:: Matrix object
 *
 */
Matrix::Matrix()
{
    logger.log("Matrix::Matrix");
}

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
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    vector<vector<int>> rows(this->maxRowsPerBlock, vector<int>(this->columnCount));
    int rowCounter = 0;
    while (getline(fin, line))
    {
        stringstream s(line);
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (!getline(s, word, ','))
                return false;
            rows[rowCounter][columnCounter] = stoi(word);
        }
        rowCounter++;
        this->rowCount++;
        if (rowCounter == this->maxRowsPerBlock)
        {
            for (int columnCounter = 0; columnCounter < this->columnCount;)
            {
                int blockColCount = min(this->columnCount - columnCounter, this->maxRowsPerBlock);
                bufferManager.writeMatrixPage(this->matrixName, this->blockCount, rows, rowCounter, columnCounter, blockColCount);
                columnCounter += blockColCount;
                this->blockCount++;
                this->dimPerBlockCount.push_back({rowCounter, blockColCount});
            }
            rowCounter = 0;
        }
    }
    fin.close();

    if (rowCounter)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount;)
        {
            int blockColCount = min(this->columnCount - columnCounter, this->maxRowsPerBlock);
            bufferManager.writeMatrixPage(this->matrixName, this->blockCount, rows, rowCounter, columnCounter, blockColCount);
            columnCounter += blockColCount;
            this->blockCount++;
            this->dimPerBlockCount.push_back({rowCounter, blockColCount});
        }
        rowCounter = 0;
    }

    if (this->rowCount == 0)
        return false;
    return true;
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
    fin.close();
    stringstream s(line);
    while (getline(s, word, ','))
    {
        this->columnCount++;
    }
    this->maxRowsPerBlock = (uint)sqrt((BLOCK_COUNT * 1024) / sizeof(int));
    this->blocksPerRow = this->columnCount / this->maxRowsPerBlock + (this->columnCount % this->maxRowsPerBlock != 0);
    return true;
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

    Cursor cursor = this->getCursor();
    vector<vector<int>> rows(this->maxRowsPerBlock, vector<int>(this->columnCount));
    for (int rowCounter = 0, blockCounter = 0; rowCounter < min(this->rowCount, (long long)count);)
    {
        int blockRowCount = this->dimPerBlockCount[blockCounter].first;
        for (int columnCounter = 0; columnCounter < this->columnCount;)
        {
            auto blockDim = this->dimPerBlockCount[blockCounter++];
            for (int blockRowCounter = 0; blockRowCounter < blockDim.first; blockRowCounter++)
            {
                if (blockRowCounter + rowCounter == count)
                {
                    this->getNextPage(&cursor);
                    break;
                }
                auto row = cursor.getNext();
                for (int blockColCounter = 0; blockColCounter < blockDim.second; blockColCounter++)
                {
                    rows[blockRowCounter][columnCounter++] = row[blockColCounter];
                }
            }
        }

        for (int blockRowCounter = 0; blockRowCounter < blockRowCount; blockRowCounter)
        {
            this->writeRow(rows[blockRowCounter], cout);
            rowCounter++;
            if (rowCounter == count)
                break;
        }
    }
    printRowCount(this->rowCount);
}

/**
 * @brief This function moves cursor to next page if next page exists.
 *
 * @param cursor 
 */
void Matrix::getNextPage(Cursor *cursor)
{
    logger.log("Matrix::getNextPage");

    if (cursor->pageIndex < this->blockCount - 1)
    {
        cursor->nextPage(cursor->pageIndex + 1);
    }
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

    Cursor cursor(this->matrixName, 0);
    cout << ":::::::::::\n";
    vector<vector<int>> rows(this->maxRowsPerBlock, vector<int>(this->columnCount));
    for (int rowwiseBlockCounter = 0, blockCounter = 0; rowwiseBlockCounter < this->blocksPerRow; rowwiseBlockCounter++)
    {
        int blockRowCount = this->dimPerBlockCount[blockCounter].first;
        for (int columnCounter = 0; columnCounter < this->columnCount;)
        {
            auto blockDim = this->dimPerBlockCount[blockCounter++];
            for (int blockRowCounter = 0; blockRowCounter < blockDim.first; blockRowCounter++)
            {
                auto row = cursor.getNext();
                for (int blockColCounter = 0; blockColCounter < blockDim.second; blockColCounter++)
                {
                    rows[blockRowCounter][columnCounter++] = row[blockColCounter];
                }
            }
        }

        for (int blockRowCounter = 0; blockRowCounter < blockRowCount; blockRowCounter)
        {
            this->writeRow(rows[blockRowCounter], fout);
        }
    }
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
 * @return Cursor 
 */
Cursor Matrix::getCursor()
{
    logger.log("Matrix::getCursor");
    Cursor cursor(this->matrixName, 0);
    return cursor;
}
