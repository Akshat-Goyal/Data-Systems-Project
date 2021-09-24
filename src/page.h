#include "logger.h"
/**
 * @brief The Page object is the main memory representation of a physical page
 * (equivalent to a block). The page class and the page.h header file are at the
 * bottom of the dependency tree when compiling files.
 *<p>
 * Do NOT modify the Page class. If you find that modifications
 * are necessary, you may do so by posting the change you want to make on Moodle
 * or Teams with justification and gaining approval from the TAs.
 *</p>
 */

class Page
{

protected:
    void fillRows();

public:
    string pageName = "";
    int pageIndex;
    int columnCount;
    int rowCount;
    vector<vector<int>> rows;
    Page();
    virtual vector<int> getRow(int rowIndex);
    virtual void writePage();
};

class MatrixPage : public Page
{
    string matrixName;

public:
    MatrixPage();
    MatrixPage(string matrixName, int pageIndex);
    MatrixPage(string matrixName, int pageIndex, vector<vector<int>> rows, int rowCount);
    // MatrixPage(string matrixName, int pageIndex, vector<vector<int>> rows, int rowCount, int colCount);
    void transpose();
    void transpose(MatrixPage *page);
    void sparseTranspose();
    void sortTwoPages(MatrixPage *page);
};

class TablePage : public Page
{
    string tableName;

public:
    TablePage();
    TablePage(string tableName, int pageIndex);
    TablePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount);
};
