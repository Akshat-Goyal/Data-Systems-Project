#include"bufferManager.h"
/**
 * @brief The cursor is an important component of the system. To read from a
 * table, you need to initialize a cursor. The cursor reads rows from a page one
 * at a time.
 *
 */
class Cursor{
    public:
    Page page;
    int pageIndex;
    int tableIndex = 1;
    string tableName;
    int pagePointer;

    public:
    Cursor(string tableName, int pageIndex);
    Cursor(string tableName, int tableIndex, int pageIndex);
    vector<int> getNextRowOfCurPage();
    vector<int> getNext();
    void nextPage(int pageIndex);
};