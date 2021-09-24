#include "cursorMatrix.h"

/**
 * @brief The Matrix class holds all information related to a loaded matrix. It
 * also implements methods that interact with the parsers, executors, cursors
 * and the buffer manager.
 */
class Matrix
{
    bool blockify();
    bool setStatistics();
    vector<int> readRowSegment(int numOfWords, ifstream &fin, bool isLastBlock);
    void writeRowSegment(vector<int> &rowSegment, int pageIndex);
    bool isSparse();
    void normalBlockify();
    void sparseBlockify();

    bool slowBlockify();
    vector<int> slowReadRowSegment(int columnPointer, int columnsInBlock, ifstream &fin);

public:
    string sourceFileName = "";
    string matrixName = "";
    uint columnCount = 0;
    long long int rowCount = 0;
    uint blockCount = 0;
    uint blocksPerRow = 0;
    uint maxRowsPerBlock = 0;
    uint numOfZeros = 0;
    bool isSparseMatrix = false;
    float SPARSE_PERCENTAGE = 0.6;
    vector<pair<uint, uint>> dimPerBlockCount;

    Matrix(string matrixName);
    bool load();
    void print();
    void printNormalMatrix();
    void printSparseMatrix();
    void transpose();
    void normalTranspose();
    void sparseTranspose();
    void makePermanent();
    void makeNormalPermanent();
    void makeSparsePermanent();
    bool isPermanent();
    void getNextPage(CursorMatrix *cursor);
    void getNextPointer(CursorMatrix *cursor);
    CursorMatrix getCursor();
    void unload();

    /**
     * @brief Static function that takes a vector of valued and prints them out in a
     * comma seperated format.
     *
     * @tparam T current usaages include int and string
     * @param row
     */
    template <typename T>
    void writeRow(vector<T> row, ostream &fout, bool first = false, bool last = true)
    {
        logger.log("Matrix::printRow");
        for (int columnCounter = 0; columnCounter < row.size(); columnCounter++)
        {
            if (!first || columnCounter != 0)
                fout << ",";
            fout << row[columnCounter];
        }
        if (last)
            fout << endl;
    }

    /**
     * @brief Static function that takes a vector of valued and prints them out in a
     * comma seperated format.
     *
     * @tparam T current usaages include int and string
     * @param row
     */
    template <typename T>
    void writeRow(vector<T> row, bool first = false, bool last = true)
    {
        logger.log("Matrix::printRow");
        ofstream fout(this->sourceFileName, ios::app);
        this->writeRow(row, fout, first, last);
        fout.close();
    }
};
