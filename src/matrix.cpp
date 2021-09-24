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
bool Matrix::slowBlockify()
{
    logger.log("Matrix::slowBlockify");
    if (!this->setStatistics())
        return false;

    vector<vector<int>> rows(this->maxRowsPerBlock);

    // TODO: change variable names: block_j to block_right for e.g.
    for (int block_j = 0, columnPointer = 0; block_j < this->blocksPerRow; block_j++)
    {
        int columnsInBlock = min(this->columnCount - columnPointer, this->maxRowsPerBlock);

        ifstream fin(this->sourceFileName, ios::in);
        int block_i = 0, rowCounter = 0;
        while (true)
        {
            rows[rowCounter] = this->slowReadRowSegment(columnPointer, columnsInBlock, fin);
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
        fin.close();
        if (rowCounter)
        {
            int blockNum = block_i * this->blocksPerRow + block_j;
            bufferManager.writeMatrixPage(this->matrixName, blockNum, rows, rowCounter);
            block_i++;
            this->dimPerBlockCount[blockNum] = {rowCounter, columnsInBlock};
            rowCounter = 0;
        }
        columnPointer += columnsInBlock;
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

// TODO: do not read line, read it word by word
vector<int> Matrix::slowReadRowSegment(int columnPointer, int columnsInBlock, ifstream &fin)
{
    logger.log("Matrix::slowReadRowSegment");
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
        {
            word.erase(remove_if(word.begin(), word.end(), ::isspace), word.end());
            row.push_back(stoi(word));
        }
    }
    return row;
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

    if (this->isSparseMatrix)
    {
        this->sparseBlockify();
    }

    else
    {
        this->normalBlockify();
    }

    if (this->rowCount == 0)
        return false;

    return true;
}

void Matrix::normalBlockify()
{
    logger.log("Matrix::normalBlockify");
    ifstream fin(this->sourceFileName, ios::in);

    for (int row = 0; row < this->rowCount; row++)
    {
        for (int block = 0; block < this->blocksPerRow; block++)
        {
            int numOfWords = this->maxRowsPerBlock;

            if (block == this->blocksPerRow - 1)
            {
                numOfWords = this->columnCount - this->maxRowsPerBlock * (this->blocksPerRow - 1);
            }

            vector<int> rowSegment = this->readRowSegment(numOfWords, fin, block == (this->blocksPerRow - 1));
            int pageIndex = (row / this->maxRowsPerBlock) * this->blocksPerRow + block;
            this->writeRowSegment(rowSegment, pageIndex);

            this->dimPerBlockCount[pageIndex].first++;
            if (this->dimPerBlockCount[pageIndex].second == 0)
            {
                this->dimPerBlockCount[pageIndex].second = numOfWords;
            }
        }
    }

    fin.close();
}

void Matrix::sparseBlockify()
{
    logger.log("Matrix::sparseBlockify");
    ifstream fin(this->sourceFileName, ios::in);

    string word;

    int numAttributes = 3;
    vector<int> row(numAttributes, 0);
    vector<vector<int>> rowsInPage(this->maxRowsPerBlock, row);
    int pageCounter = 0;
    int curPageIndex = 0;

    for (int i = 0; i < this->columnCount * this->columnCount; i++)
    {
        if ((i + 1) % this->columnCount != 0)
        {
            if (getline(fin, word, ','))
            {
                word.erase(remove_if(word.begin(), word.end(), ::isspace), word.end());

                if (stoi(word) != 0)
                {
                    rowsInPage[pageCounter][0] = i / this->columnCount;
                    rowsInPage[pageCounter][1] = i % this->columnCount;
                    rowsInPage[pageCounter][2] = stoi(word);
                    pageCounter++;
                }
            }
        }

        else
        {
            if (getline(fin, word, '\n'))
            {
                word.erase(remove_if(word.begin(), word.end(), ::isspace), word.end());

                if (stoi(word) != 0)
                {
                    rowsInPage[pageCounter][0] = i / this->columnCount;
                    rowsInPage[pageCounter][1] = i % this->columnCount;
                    rowsInPage[pageCounter][2] = stoi(word);
                    pageCounter++;
                }
            }
        }

        if (pageCounter == this->maxRowsPerBlock)
        {
            bufferManager.writeMatrixPage(this->matrixName, curPageIndex, rowsInPage, pageCounter);
            this->dimPerBlockCount[curPageIndex] = {pageCounter, numAttributes};
            curPageIndex++;
            pageCounter = 0;
        }
    }

    if (pageCounter)
    {
        bufferManager.writeMatrixPage(this->matrixName, curPageIndex, rowsInPage, pageCounter);
        this->dimPerBlockCount[curPageIndex] = {pageCounter, numAttributes};
        curPageIndex++;
        pageCounter = 0;
    }

    fin.close();
}

/**
 * @brief This function reads the segment of the row pointed by fin.
 *
 * @return vector<int>
 */

vector<int> Matrix::readRowSegment(int numOfWords, ifstream &fin, bool isLastBlock)
{
    logger.log("Matrix::readRowSegment");
    vector<int> row;
    string word;

    for (int wordCounter = 0; wordCounter < numOfWords - 1; wordCounter++)
    {
        if (getline(fin, word, ','))
        {
            word.erase(remove_if(word.begin(), word.end(), ::isspace), word.end());
            row.push_back(stoi(word));
        }
    }

    if (!isLastBlock)
    {
        if (getline(fin, word, ','))
        {
            word.erase(remove_if(word.begin(), word.end(), ::isspace), word.end());
            row.push_back(stoi(word));
        }
    }

    else
    {
        if (getline(fin, word, '\n'))
        {
            word.erase(remove_if(word.begin(), word.end(), ::isspace), word.end());
            row.push_back(stoi(word));
        }
    }

    return row;
}

/**
 * @brief This function appends a row segment to the page with given index.
 */

void Matrix::writeRowSegment(vector<int> &rowSegment, int pageIndex)
{
    logger.log("Matrix::writeRowSegment");

    string pageName = "../data/temp/" + this->matrixName + "_Page" + to_string(pageIndex);

    ofstream fout;
    fout.open(pageName, ios::out | ios::app);

    for (int ind = 0; ind < rowSegment.size(); ind++)
    {
        if (ind != 0)
            fout << " ";
        fout << rowSegment[ind];
    }
    fout << endl;
    fout.close();
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
    {
        fin.close();
        return false;
    }
    fin.close();
    stringstream s(line);
    while (getline(s, word, ','))
        this->columnCount++;

    this->isSparseMatrix = this->isSparse();

    if (!this->isSparseMatrix)
    {
        // TODO: sizeof(int) + 1? (1 for spaces): not needed prolly
        this->maxRowsPerBlock = (uint)sqrt((BLOCK_SIZE * 1024) / sizeof(int));
        this->blocksPerRow = this->columnCount / this->maxRowsPerBlock + (this->columnCount % this->maxRowsPerBlock != 0);
        this->blockCount = this->blocksPerRow * this->blocksPerRow;
        this->rowCount = this->columnCount;
        this->dimPerBlockCount.assign(this->blockCount, {0, 0});
    }

    else
    {
        this->maxRowsPerBlock = (uint)(BLOCK_SIZE * 1024) / (sizeof(int) * 3);
        this->rowCount = this->columnCount;
        this->blockCount = ((this->columnCount * this->columnCount) - this->numOfZeros + this->maxRowsPerBlock - 1) / this->maxRowsPerBlock;
        this->dimPerBlockCount.assign(this->blockCount, {0, 0});
    }

    return true;
}

bool Matrix::isSparse()
{
    logger.log("Matrix::isSparse");
    ifstream fin(this->sourceFileName, ios::in);

    string word;

    int numOfZeros = 0;

    for (int i = 0; i < this->columnCount * this->columnCount; i++)
    {
        if ((i + 1) % this->columnCount != 0)
        {
            if (getline(fin, word, ','))
            {
                word.erase(remove_if(word.begin(), word.end(), ::isspace), word.end());
                numOfZeros += (stoi(word) == 0);
            }
        }

        else
        {
            if (getline(fin, word, '\n'))
            {
                word.erase(remove_if(word.begin(), word.end(), ::isspace), word.end());
                numOfZeros += (stoi(word) == 0);
            }
        }
    }

    float zeros_percentage = (float)numOfZeros / (float)(this->columnCount * this->columnCount);
    fin.close();

    // cout << numOfZeros << " " << zeros_percentage << " " << (int)(zeros_percentage >= SPARSE_PERCENTAGE) << endl;

    this->numOfZeros = numOfZeros;

    return zeros_percentage >= SPARSE_PERCENTAGE;
}

/**
 * @brief Transposes matrix by swaping block_ij and block_ji elements for each i, j.
 *
 */

void Matrix::transpose()
{
    logger.log("Matrix::transpose");
    if (this->isSparseMatrix)
    {
        this->sparseTranspose();
    }

    else
    {
        this->normalTranspose();
    }
}

void Matrix::normalTranspose()
{
    logger.log("Matrix::normalTranspose");

    for (int block_i = 0; block_i < this->blocksPerRow; block_i++)
    {
        for (int block_j = block_i; block_j < this->blocksPerRow; block_j++)
        {
            if (block_i != block_j)
            {
                int block_ij = block_i * this->blocksPerRow + block_j, block_ji = block_j * this->blocksPerRow + block_i;
                MatrixPage *page_ij = bufferManager.getMatrixPage(this->matrixName, block_ij);
                MatrixPage *page_ji = bufferManager.getMatrixPage(this->matrixName, block_ji);
                page_ij = bufferManager.getMatrixPage(this->matrixName, block_ij);

                page_ij->transpose(page_ji);
                page_ij->writePage();
                page_ji->writePage();
            }
            else
            {
                int block_ij = block_i * this->blocksPerRow + block_j;
                MatrixPage *page_ij = bufferManager.getMatrixPage(this->matrixName, block_ij);
                page_ij->transpose();
                page_ij->writePage();
            }
        }
    }
}

void Matrix::sparseTranspose()
{
    logger.log("Matrix::sparseTranspose");
    for (int block = 0; block < this->blockCount; block++)
    {
        MatrixPage *curPage = bufferManager.getMatrixPage(this->matrixName, block);
        curPage->sparseTranspose();
        curPage->writePage();
    }

    for (int block_i = 0; block_i < this->blockCount; block_i++)
    {

        for (int block_j = block_i + 1; block_j < this->blockCount; block_j++)
        {
            MatrixPage *page_j = bufferManager.getMatrixPage(this->matrixName, block_j);
            MatrixPage *page_i = bufferManager.getMatrixPage(this->matrixName, block_i);
            page_j = bufferManager.getMatrixPage(this->matrixName, block_j);

            // cout << "before sort " << block_i << " " << block_j << " " << this->blockCount << endl;
            page_i->sortTwoPages(page_j);
            // cout << "after sort " << block_i << " " << block_j << " " << this->blockCount << endl;

            page_i->writePage();
            page_j->writePage();
        }

    }
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

    if (this->isSparseMatrix)
    {
        this->printSparseMatrix();
    }

    else
    {
        this->printNormalMatrix();
    }
}

void Matrix::printNormalMatrix()
{
    logger.log("Matrix::printNormalMatrix");
    uint count = min((long long)PRINT_COUNT, this->rowCount);

    CursorMatrix cursor = this->getCursor();
    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        int remaining = count;
        bool last = false;

        for (int seg = 0; remaining != 0 && seg < this->blocksPerRow; seg++)
        {
            vector<int> nextSegment = cursor.getNext();

            if (nextSegment.size() >= remaining)
            {
                nextSegment.resize(remaining);
                last = true;
            }

            remaining -= nextSegment.size();
            
            this->writeRow(nextSegment, cout, seg == 0, last);

            if (!remaining && seg != this->blocksPerRow - 1 && rowCounter != count - 1)
            {
                int lastSegBlockNum = cursor.pageIndex - cursor.pageIndex % this->blocksPerRow + this->blocksPerRow - 1;
                cursor.nextPage(lastSegBlockNum, cursor.pagePointer);
                cursor.getNext();
            }
        }
    }
    printRowCount(this->rowCount);
}

void Matrix::printSparseMatrix()
{
    logger.log("Matrix::printSparseMatrix");
    uint count = min((long long)PRINT_COUNT, this->rowCount);

    CursorMatrix cursor = this->getCursor();
    vector<int> curTuple = cursor.getNext();

    while (curTuple.size() > 0 && curTuple[1] >= count)
    {
        curTuple = cursor.getNext();
    }

    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        vector<int> curRow;

        for (int colCounter = 0; colCounter < count; colCounter++)
        {
            int value = 0;
            if (curTuple.size() > 0 && curTuple[0] == rowCounter && curTuple[1] == colCounter)
            {
                value = curTuple[2];
                curTuple = cursor.getNext();

                while (curTuple.size() > 0 && curTuple[1] >= count)
                {
                    curTuple = cursor.getNext();
                }
            }

            curRow.push_back(value);
        }

        this->writeRow(curRow, cout, 1, 1);
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

    // pageIndex = block number of the page, pagePointer = row number within the block

    if ((cursor->pageIndex + 1) % this->blocksPerRow == 0)
    {
        if (cursor->pagePointer == this->dimPerBlockCount[cursor->pageIndex].first - 1)
        {
            if (cursor->pageIndex < this->blockCount - 1)
                cursor->nextPage(cursor->pageIndex + 1);
        }

        // nextPage (dubious variable name): given page index, return the page with that page index
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

    if (!this->isSparseMatrix)
    {
        this->makeNormalPermanent();
    }

    else
    {
        this->makeSparsePermanent();
    }
}

void Matrix::makeNormalPermanent()
{
    logger.log("Matrix::makeNormalPermanent");

    string newSourceFile = "../data/" + this->matrixName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    CursorMatrix cursor = this->getCursor();
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int seg = 0; seg < this->blocksPerRow; seg++)
            this->writeRow(cursor.getNext(), fout, seg == 0, seg == this->blocksPerRow - 1);
    }
    fout.close();
}

void Matrix::makeSparsePermanent()
{
    logger.log("Matrix::makeSparsePermanent");

    string newSourceFile = "../data/" + this->matrixName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    CursorMatrix cursor = this->getCursor();
    vector<int> curTuple = cursor.getNext();

    for (int rowCounter = 0; rowCounter < this->columnCount; rowCounter++)
    {
        for (int colCounter = 0; colCounter < this->columnCount; colCounter++)
        {
            int value = 0;
            if (curTuple.size() > 0 && curTuple[0] == rowCounter && curTuple[1] == colCounter)
            {
                value = curTuple[2];
                curTuple = cursor.getNext();
            }

            if (colCounter != 0)
            {
                fout << ",";
            }

            fout << value;
        }

        fout << endl;
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
 * @return CursorMatrix
 */
CursorMatrix Matrix::getCursor()
{
    logger.log("Matrix::getCursor");
    CursorMatrix cursor(this->matrixName, 0);
    return cursor;
}
