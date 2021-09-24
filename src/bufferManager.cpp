#include "global.h"

BufferManager::BufferManager()
{
    logger.log("BufferManager::BufferManager");
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param tableName
 * @param pageIndex
 * @return TablePage
 */
TablePage BufferManager::getPage(string tableName, int pageIndex)
{
    logger.log("BufferManager::getPage");
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    if (this->inPool(pageName))
        return *this->getFromPool(pageName);
    else
        return *this->insertIntoPool(tableName, pageIndex);
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param matrixName
 * @param pageIndex
 * @return MatrixPage*
 */
MatrixPage *BufferManager::getMatrixPage(string matrixName, int pageIndex)
{
    logger.log("BufferManager::getMatrixPage");
    string pageName = "../data/temp/" + matrixName + "_Page" + to_string(pageIndex);
    // cout << matrixName << " " << pageIndex << " reached buffer manager get matrix page" << endl;
    if (this->inPool(pageName))
        return this->getMatrixFromPool(pageName);
    else
        return this->insertMatrixIntoPool(matrixName, pageIndex);
}

/**
 * @brief Checks to see if a page exists in the pool
 *
 * @param pageName
 * @return true
 * @return false
 */
bool BufferManager::inPool(string pageName)
{
    logger.log("BufferManager::inPool");
    for (auto *page : this->pages)
    {
        if (pageName == page->pageName)
            return true;
    }
    return false;
}

/**
 * @brief If the page is present in the pool, then this function returns the
 * page. Note that this function will fail if the page is not present in the
 * pool or page type is not table.
 *
 * @param pageName
 * @return TablePage*
 */

// TODO: change name to getTableFromPool
TablePage *BufferManager::getFromPool(string pageName)
{
    logger.log("BufferManager::getFromPool");
    for (auto page : this->pages)
        if (pageName == page->pageName)
            return dynamic_cast<TablePage *>(page);
}

/**
 * @brief If the page is present in the pool, then this function returns the
 * page. Note that this function will fail if the page is not present in the
 * pool or page type is not matrix.
 *
 * @param pageName
 * @return MatrixPage*
 */
MatrixPage *BufferManager::getMatrixFromPool(string pageName)
{
    logger.log("BufferManager::getMatrixFromPool");
    for (auto page : this->pages)
        if (pageName == page->pageName)
            return dynamic_cast<MatrixPage *>(page);
}

/**
 * @brief Inserts page indicated by tableName and pageIndex into pool. If the
 * pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure.
 *
 * @param tableName
 * @param pageIndex
 * @return TablePage*
 */
TablePage *BufferManager::insertIntoPool(string tableName, int pageIndex)
{
    logger.log("BufferManager::insertIntoPool");
    TablePage *page = new TablePage(tableName, pageIndex);
    if (this->pages.size() >= BLOCK_COUNT)
    {
        delete pages.front();
        pages.pop_front();
    }
    pages.push_back(page);
    return page;
}

/**
 * @brief Inserts page indicated by matrixName and pageIndex into pool. If the
 * pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure.
 *
 * @param tableName
 * @param pageIndex
 * @return MatrixPage*
 */
MatrixPage *BufferManager::insertMatrixIntoPool(string matrixName, int pageIndex)
{
    logger.log("BufferManager::insertMatrixIntoPool");
    MatrixPage *page = new MatrixPage(matrixName, pageIndex);
    if (this->pages.size() >= BLOCK_COUNT)
    {
        delete pages.front();
        pages.pop_front();
    }
    pages.push_back(page);
    return page;
}

/**
 * @brief The buffer manager is also responsible for writing pages. This is
 * called when new tables are created using assignment statements or load.
 *
 * @param tableName
 * @param pageIndex
 * @param rows
 * @param rowCount
 */
void BufferManager::writePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("BufferManager::writePage");
    TablePage page(tableName, pageIndex, rows, rowCount);
    page.writePage();
}

/**
 * @brief The buffer manager is also responsible for writing pages. This is
 * called when new matrices are created using load.
 *
 * @param tableName
 * @param pageIndex
 * @param rows
 * @param rowCount
 * @param colStartIdx
 * @param columnCount
 */
void BufferManager::writeMatrixPage(string matrixName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("BufferManager::writeMatrixPage");
    MatrixPage page(matrixName, pageIndex, rows, rowCount);
    page.writePage();
}

// void BufferManager::writeMatrixPage(string matrixName, int pageIndex, vector<vector<int>> rows, int rowCount, int colCount)
// {
//     logger.log("BufferManager::writeMatrixPage");
//     MatrixPage page(matrixName, pageIndex, rows, rowCount, colCount);
//     page.writePage();
// }

/**
 * @brief Deletes file names fileName
 *
 * @param fileName
 */
void BufferManager::deleteFile(string fileName)
{

    if (remove(fileName.c_str()))
        logger.log("BufferManager::deleteFile: Err");
    else
        logger.log("BufferManager::deleteFile: Success");
}

/**
 * @brief Overloaded function that calls deleteFile(fileName) by constructing
 * the fileName from the relationName and pageIndex.
 *
 * @param tableName
 * @param pageIndex
 */
void BufferManager::deleteFile(string relationName, int pageIndex)
{
    logger.log("BufferManager::deleteFile");
    string fileName = "../data/temp/" + relationName + "_Page" + to_string(pageIndex);
    this->deleteFile(fileName);
}

BufferManager::~BufferManager()
{
    logger.log("BufferManager::~BufferManager");
    while (!this->pages.empty())
    {
        delete this->pages.front();
        this->pages.pop_front();
    }
}
