#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- SORT <table_name> BY <column_name> IN ASC | DESC [BUFFER <buffer_size>]
 */
bool syntacticParseSORT()
{
    logger.log("syntacticParseSORT");
    if ((tokenizedQuery.size() != 8 && tokenizedQuery.size() != 10) || tokenizedQuery[4] != "BY" || tokenizedQuery[6] != "IN")
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }

    if (tokenizedQuery.size() == 10) {
        regex numeric("[+]?[0-9]+");
        string bufferSize = tokenizedQuery[9];
        if (tokenizedQuery[8] != "BUFFER" || !regex_match(bufferSize, numeric))
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        else
        {
            parsedQuery.sortBufferSize = stoi(bufferSize);
        }
    }

    parsedQuery.queryType = SORT;
    parsedQuery.sortResultRelationName = tokenizedQuery[0];
    parsedQuery.sortRelationName = tokenizedQuery[3];
    parsedQuery.sortColumnName = tokenizedQuery[5];

    string sortingStrategy = tokenizedQuery[7];
    if (sortingStrategy == "ASC")
        parsedQuery.sortingStrategy = ASC;
    else if (sortingStrategy == "DESC" || sortingStrategy == "DSC")
        parsedQuery.sortingStrategy = DESC;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseSORT()
{
    logger.log("semanticParseSORT");

    if (tableCatalogue.isTable(parsedQuery.sortResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnName, parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if (parsedQuery.sortBufferSize < 3)
    {
        cout << "SEMANTIC ERROR: Buffer size can't be less than 3" << endl;
        return false;
    }
    return true;
}

void executeSORTPhase1()
{
    logger.log("executeSORTPhase1");

    Table table = *tableCatalogue.getTable(parsedQuery.sortRelationName);
    Cursor cursor = table.getCursor();
    int sortColumnIndex = table.getColumnIndex(parsedQuery.sortColumnName);
    Table *resultantTable = new Table(parsedQuery.sortRelationName, table.columns);

    int nb = parsedQuery.sortBufferSize - 1;
    vector<vector<int>> resultantRows(resultantTable->maxRowsPerBlock, vector<int>(resultantTable->columnCount, 0));

    for (resultantTable->blockCount = 0; resultantTable->blockCount < table.blockCount;)
    {
        multimap<int, vector<int>> sortedRows;
        for (int i = resultantTable->blockCount; i < min(resultantTable->blockCount + nb, table.blockCount); i++)
        {
            vector<int> row = cursor.getNextRowOfCurPage();
            while (!row.empty())
            {
                sortedRows.insert({row[sortColumnIndex], row});
                row = cursor.getNextRowOfCurPage();
            }
            if (!table.getNextPage(&cursor))
                break;
        }

        int rowCounter = 0;
        auto it = sortedRows.begin();
        while (it != sortedRows.end())
        {
            resultantRows[rowCounter++] = it++->second;
            if (rowCounter == resultantTable->rowCount)
            {
                bufferManager.writePage(resultantTable->tableName, resultantTable->blockCount, resultantRows, rowCounter);
                resultantTable->rowCount += rowCounter;
                resultantTable->blockCount++;
                resultantTable->rowsPerBlockCount.emplace_back(rowCounter);
                rowCounter = 0;
            }
        }
    }
}

void executeSORTPhase2()
{
    
}

void executeSORT()
{
    logger.log("executeSORT");
    executeSORTPhase1();
    executeSORTPhase2();
    return;
}
