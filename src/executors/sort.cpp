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

void executeSORTPhase1(Table &table, Table *resultantTable)
{
    logger.log("executeSORTPhase1");

    Cursor cursor = table.getCursor();
    int sortColumnIndex = table.getColumnIndex(parsedQuery.sortColumnName);

    int nb = parsedQuery.sortBufferSize - 1;
    int nr = ceil((double)table.blockCount / (double)nb);

    vector<vector<int>> resultantRows(resultantTable->maxRowsPerBlock, vector<int>(resultantTable->columnCount, 0));

    for (resultantTable->blockCount = 0; resultantTable->blockCount < table.blockCount;)
    {
        multimap<int, vector<int>> sortRows;
        int sortingOrder = parsedQuery.sortingStrategy == ASC ? 1 : -1;
        for (int i = resultantTable->blockCount; i < min(resultantTable->blockCount + nb, table.blockCount); i++)
        {
            vector<int> row = cursor.getNextRowOfCurPage();
            while (!row.empty())
            {
                sortRows.insert({sortingOrder * row[sortColumnIndex], row});
                row = cursor.getNextRowOfCurPage();
            }
            if (!table.getNextPage(&cursor))
                break;
        }

        int rowCounter = 0;
        auto it = sortRows.begin();
        while (it != sortRows.end())
        {
            resultantRows[rowCounter++] = it++->second;
            if (rowCounter == resultantTable->maxRowsPerBlock)
            {
                bufferManager.writePage(resultantTable->tableName, nr, resultantTable->blockCount, resultantRows, rowCounter);
                resultantTable->rowCount += rowCounter;
                resultantTable->blockCount++;
                resultantTable->rowsPerBlockCount.emplace_back(rowCounter);
                rowCounter = 0;
            }
        }
        if (rowCounter)
        {
            bufferManager.writePage(resultantTable->tableName, nr, resultantTable->blockCount, resultantRows, rowCounter);
            resultantTable->rowCount += rowCounter;
            resultantTable->blockCount++;
            resultantTable->rowsPerBlockCount.emplace_back(rowCounter);
            rowCounter = 0;
        }
    }
    tableCatalogue.insertTable(resultantTable);
}

void executeSORTPhase2(Table &table, Table *resultantTable)
{
    logger.log("executeSORTPhase2");

    int sortColumnIndex = table.getColumnIndex(parsedQuery.sortColumnName);
    int nb = parsedQuery.sortBufferSize - 1;
    int nr = ceil((double)resultantTable->blockCount / (double)nb);
    int pnr = nb;
    
    int rowCounter = 0;
    vector<vector<int>> resultantRows(resultantTable->maxRowsPerBlock, vector<int>(resultantTable->columnCount, 0));

    while (nr != 1)
    {
        int next_nr = ceil((double)nr / (double)nb);
        int blockCounter = 0;
        for (int i = 0; i < nr; i += nb)
        {
            multimap<int, pair<vector<int>, Cursor*>> mergeRows;
            int sortingOrder = parsedQuery.sortingStrategy == ASC ? 1 : -1;
            for (int j = i; j < min(nr, i + nb); j++)
            {
                Cursor *cursor = new Cursor(resultantTable->tableName, nr, j * pnr);
                vector<int> row = cursor->getNextRowOfCurPage();
                mergeRows.insert({sortingOrder * row[sortColumnIndex], {row, cursor}});
            }

            while (!mergeRows.empty())
            {
                auto it = mergeRows.begin();
                vector<int> row = it->second.first;
                Cursor *cursor = it->second.second;
                mergeRows.erase(it);

                resultantRows[rowCounter++] = row;
                if (rowCounter == resultantTable->maxRowsPerBlock)
                {
                    bufferManager.writePage(resultantTable->tableName, next_nr, blockCounter++, resultantRows, rowCounter);
                    rowCounter = 0;
                }
                row = cursor->getNextRowOfCurPage();
                if (row.empty())
                {
                    if ((cursor->pageIndex + 1) % pnr && (cursor->pageIndex + 1) != resultantTable->blockCount)
                    {
                        cursor->nextPage(cursor->pageIndex + 1);
                        row = cursor->getNextRowOfCurPage();
                    }
                    else
                    {
                        delete cursor;
                    }
                }
                if (!row.empty())
                {
                    mergeRows.insert({sortingOrder * row[sortColumnIndex], {row, cursor}});
                }
            }
            if (rowCounter)
            {
                bufferManager.writePage(resultantTable->tableName, next_nr, blockCounter++, resultantRows, rowCounter);
                rowCounter = 0;
            }
        }

        for (int pageCounter = 0; pageCounter < resultantTable->blockCount; pageCounter++)
        {
            bufferManager.deleteFile(resultantTable->tableName, nr, pageCounter);
        }
        nr = next_nr;
        pnr = pnr * nb;
    }
}

void executeSORT()
{
    logger.log("executeSORT");

    Table table = *tableCatalogue.getTable(parsedQuery.sortRelationName);
    Table *resultantTable = new Table(parsedQuery.sortResultRelationName, table.columns);
    executeSORTPhase1(table, resultantTable);
    executeSORTPhase2(table, resultantTable);
    return;
}
