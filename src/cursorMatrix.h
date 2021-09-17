#include"bufferManager.h"
/**
 * @brief The cursor is an important component of the system. To read from a
 * matrix, you need to initialize a cursor. The cursor reads rows from a page one
 * at a time.
 *
 */
class CursorMatrix{
    public:
    MatrixPage page;
    int pageIndex;
    string matrixName;
    int pagePointer;

    public:
    CursorMatrix(string matrixName, int pageIndex);
    vector<int> getNext();
    vector<int> getNextPageRow();
    void nextPage(int pageIndex, int pagePointer = 0);
};
