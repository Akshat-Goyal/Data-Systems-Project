#include "global.h"
/**
 * @brief Construct a new Page object. Never used as part of the code
 *
 */
Page::Page()
{
    logger.log("Page::Page");
    this->pageName = "";
    this->pageIndex = -1;
    this->rowCount = 0;
    this->columnCount = 0;
    this->rows.clear();
}

TablePage::TablePage()
{
    logger.log("TablePage::TablePage1");
    this->tableName = "";
}

MatrixPage::MatrixPage()
{
    logger.log("MatrixPage::MatrixPage1");
    this->matrixName = "";
}

/**
 * @brief Construct a new Page:: Page object given the table name and page
 * index. When tables are loaded they are broken up into blocks of BLOCK_SIZE
 * and each block is stored in a different file named
 * "<tablename>_Page<pageindex>". For example, If the Page being loaded is of
 * table "R" and the pageIndex is 2 then the file name is "R_Page2". The page
 * loads the rows (or tuples) into a vector of rows (where each row is a vector
 * of integers).
 *
 * @param tableName
 * @param pageIndex
 */
TablePage::TablePage(string tableName, int pageIndex)
{
    logger.log("TablePage::TablePage2");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
    Table table = *tableCatalogue.getTable(tableName);
    this->columnCount = table.columnCount;
    this->rowCount = table.rowsPerBlockCount[pageIndex];
    uint maxRowCount = table.maxRowsPerBlock;
    this->rows.assign(maxRowCount, vector<int>(columnCount));
    this->fillRows();
}

TablePage::TablePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("TablePage::TablePage3");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->rows = rows;
    this->rowCount = rowCount;
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
}

/**
 * @brief Construct a new Page:: Page object given the matrix name and page
 * index. When NxN matrix is loaded it is broken up into smaller MxM matrices
 * such that each smaller matrix can fit in a block and each block is stored
 * in a different file named "<matrixname>_Page<pageindex>". For example, If
 * the Page being loaded is of matrix "R" and the pageIndex is 2 then the file
 * name is "R_Page2". The page loads the rows (or tuples) into a vector of rows
 * (where each row is a vector of integers).
 *
 * @param matrixName
 * @param pageIndex
 */
MatrixPage::MatrixPage(string matrixName, int pageIndex)
{
    logger.log("MatrixPage::MatrixPage2");
    this->matrixName = matrixName;
    this->pageIndex = pageIndex;

    // cout << "pageIndex is " << this->pageIndex << " " << pageIndex << endl;
    this->pageName = "../data/temp/" + this->matrixName + "_Page" + to_string(pageIndex);
    Matrix *matrix = matrixCatalogue.getMatrix(matrixName);
    uint maxRowCount = matrix->maxRowsPerBlock;
    this->rowCount = matrix->dimPerBlockCount[pageIndex].first;
    this->columnCount = matrix->dimPerBlockCount[pageIndex].second;
    this->rows.assign(this->rowCount, vector<int>(this->columnCount));
    this->fillRows();
}

MatrixPage::MatrixPage(string matrixName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("MatrixPage::MatrixPage3");
    this->matrixName = matrixName;
    this->pageIndex = pageIndex;
    // cout << "pageIndex is " << this->pageIndex << " " << pageIndex << endl;

    this->rows = rows;
    this->rowCount = rowCount;
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/" + this->matrixName + "_Page" + to_string(pageIndex);
}

/**
 * @brief Transposes rows of a single page
 *
 */
void MatrixPage::transpose()
{
    logger.log("MatrixPage::transpose");
    for (int i = 0; i < this->rowCount; i++)
    {
        for (int j = i + 1; j < this->columnCount; j++)
        {
            swap(this->rows[i][j], this->rows[j][i]);
        }
    }
}

bool cmp(const vector<int> &a, const vector<int> &b)
{
    if (a[0] == b[0])
    {
        return a[1] < b[1];
    }

    return a[0] < b[0];
}

void MatrixPage::sparseTranspose()
{
    logger.log("MatrixPage::sparseTranspose");

    for (int i = 0; i < this->rowCount; i++)
    {
        swap(this->rows[i][0], this->rows[i][1]);
    }

    sort(this->rows.begin(), this->rows.end(), cmp);
}

void MatrixPage::sortTwoPages(MatrixPage *page)
{
    logger.log("MatrixPage::sortTwoPages");

    // cout << this->rowCount << " " << page->rowCount << endl;

    vector<vector<int>> allRows;

    // cout << this->rows.size() << " " << page->rows.size() << endl;

    for (int i = 0; i < this->rowCount; i++)
    {
        allRows.push_back(this->rows[i]);
    }

    for (int i = 0; i < page->rowCount; i++)
    {
        allRows.push_back(page->rows[i]);
    }

    // cout << allRows.size() << " reached midway in sort two pages" << endl;

    sort(allRows.begin(), allRows.end(), cmp);

    for (int i = 0; i < this->rowCount; i++)
    {
        this->rows[i] = allRows[i];
    }

    for (int i = 0; i < page->rowCount; i++)
    {
        page->rows[i] = allRows[this->rowCount + i];
    }

    // allRows.clear();
}

/**
 * @brief Given two pages, it transposes both of them without using extra memory, by swapping (i, j)
 * of the first page with (j, i) of the second page
 *
 */
void MatrixPage::transpose(MatrixPage *page)
{
    logger.log("MatrixPage::transpose");
    for (int i = 0; i < this->rowCount; i++)
    {
        for (int j = 0; j < this->columnCount; j++)
        {
            swap(this->rows[i][j], page->rows[j][i]);
        }
    }
}

/**
 * @brief A single page is loaded into rows
 *
 */
void Page::fillRows()
{
    logger.log("Page::fillRows");
    ifstream fin(this->pageName, ios::in);
    int number;

    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            fin >> number;
            this->rows[rowCounter][columnCounter] = number;
        }
    }
    fin.close();
}

/**
 * @brief Get row from page indexed by rowIndex
 *
 * @param rowIndex
 * @return vector<int>
 */
vector<int> Page::getRow(int rowIndex)
{
    logger.log("Page::getRow");
    vector<int> result;
    result.clear();
    if (rowIndex >= this->rowCount)
        return result;
    return this->rows[rowIndex];
}

/**
 * @brief writes current page contents to file.
 *
 */
void Page::writePage()
{
    logger.log("Page::writePage");

    // cout << "write page started " << this->pageIndex << " " << this->rowCount << " " << this->columnCount << endl;
    ofstream fout(this->pageName, ios::trunc);
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (columnCounter != 0)
                fout << " ";
            fout << this->rows[rowCounter][columnCounter];
        }
        fout << endl;
    }
    fout.close();

    // cout << "write page completed " << this->pageIndex << endl;
}
