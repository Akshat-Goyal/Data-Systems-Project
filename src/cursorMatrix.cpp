#include "global.h"

CursorMatrix::CursorMatrix(string matrixName, int pageIndex)
{
    logger.log("CursorMatrix::CursorMatrix");
    this->page = *bufferManager.getMatrixPage(matrixName, pageIndex);
    this->pagePointer = 0;
    this->matrixName = matrixName;
    this->pageIndex = pageIndex;
}

/**
 * @brief This function reads the next row from the page. The index of the
 * current row read from the page is indicated by the pagePointer(points to row
 * in page the cursor is pointing to).
 *
 * @return vector<int>
 */
vector<int> CursorMatrix::getNextPageRow()
{
    logger.log("CursorMatrix::getNextPageRow");
    vector<int> result = this->page.getRow(this->pagePointer);
    this->pagePointer++;
    if (result.empty())
    {
        matrixCatalogue.getMatrix(this->matrixName)->getNextPage(this);
        if (!this->pagePointer)
        {
            result = this->page.getRow(this->pagePointer);
            this->pagePointer++;
        }
    }
    return result;
}

/**
 * @brief This function reads the next segment of the matrix's row from the page.
 * If the row is fully read, it moves to first segment of the next row. The index
 * of the current segment read from the page is indicated by the pagePointer
 * (points to row in page the cursor is pointing to).
 *
 * @return vector<int>
 */
vector<int> CursorMatrix::getNext()
{
    logger.log("CursorMatrix::getNext");

    vector<int> result = this->page.getRow(this->pagePointer);

    if (matrixCatalogue.getMatrix(this->matrixName)->isSparseMatrix)
    {
        return this->getNextPageRow();
    }

    else
    {
        matrixCatalogue.getMatrix(this->matrixName)->getNextPointer(this);
    }

    return result;
}

/**
 * @brief Function that loads Page indicated by pageIndex. Now the cursor starts
 * reading from the new page from the row pointed by pagePointer.
 *
 * @param pageIndex
 */
void CursorMatrix::nextPage(int pageIndex, int pagePointer)
{
    logger.log("CursorMatrix::nextPage");
    if (this->pageIndex != pageIndex)
        this->page = *bufferManager.getMatrixPage(this->matrixName, pageIndex);
    this->pageIndex = pageIndex;

    this->pagePointer = pagePointer;
}
