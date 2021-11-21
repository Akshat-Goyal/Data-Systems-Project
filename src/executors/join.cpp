#include "global.h"
/**
 * @brief
 * SYNTAX: R <- JOIN USING <join_algorithm> <relation_name1>, <relation_name2> ON <column_name1> <bin_op> <column_name2> BUFFER <buffer_size>
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");

    if (tokenizedQuery.size() != 13 || tokenizedQuery[3] != "USING" || tokenizedQuery[7] != "ON" || tokenizedQuery[11] != "BUFFER")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinFirstRelationName = tokenizedQuery[5];
    parsedQuery.joinSecondRelationName = tokenizedQuery[6];
    parsedQuery.joinFirstColumnName = tokenizedQuery[8];
    parsedQuery.joinSecondColumnName = tokenizedQuery[10];

    string joinAlgorithm = tokenizedQuery[4];
    if (joinAlgorithm == "NESTED")
        parsedQuery.joinAlgorithm = NESTED;
    else if (joinAlgorithm == "PARTHASH")
        parsedQuery.joinAlgorithm = PARTHASH;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    string binaryOperator = tokenizedQuery[9];
    if (binaryOperator == "==")
        parsedQuery.joinBinaryOperator = EQUAL;
    // else if (binaryOperator == "<")
    //     parsedQuery.joinBinaryOperator = LESS_THAN;
    // else if (binaryOperator == ">")
    //     parsedQuery.joinBinaryOperator = GREATER_THAN;
    // else if (binaryOperator == ">=" || binaryOperator == "=>")
    //     parsedQuery.joinBinaryOperator = GEQ;
    // else if (binaryOperator == "<=" || binaryOperator == "=<")
    //     parsedQuery.joinBinaryOperator = LEQ;
    // else if (binaryOperator == "!=")
    //     parsedQuery.joinBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    regex numeric("[+]?[0-9]+");
    string bufferSize = tokenizedQuery[12];
    if (regex_match(bufferSize, numeric))
    {
        parsedQuery.joinBufferSize = stoi(bufferSize);
    }
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if (parsedQuery.joinBufferSize < 3)
    {
        cout << "SEMANTIC ERROR: Buffer size can't be less than 3" << endl;
        return false;
    }
    return true;
}

Table *createJoinTable(Table *table1, Table *table2)
{
    vector<string> columnList = table1->columns;
    columnList.insert(columnList.end(), table2->columns.begin(), table2->columns.end());
    return new Table(parsedQuery.joinResultRelationName, columnList);
}

void executeNestedJoin()
{
    logger.log("executeNestedJoin");

    Table table1 = *tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table table2 = *tableCatalogue.getTable(parsedQuery.joinSecondRelationName);
    Table *resultantTable = createJoinTable(&table1, &table2);

    bool isTable1Outside = true;
    if (table1.blockCount > table2.blockCount)
    {
        isTable1Outside = false;
        swap(table1, table2);
        swap(parsedQuery.joinFirstColumnName, parsedQuery.joinSecondColumnName);
    }

    Cursor cursor1 = table1.getCursor();
    int joinFirstColumnIndex = table1.getColumnIndex(parsedQuery.joinFirstColumnName);
    int joinSecondColumnIndex = table2.getColumnIndex(parsedQuery.joinSecondColumnName);

    int rowCounter = 0;
    vector<vector<int>> resultantRows(resultantTable->maxRowsPerBlock, vector<int>(resultantTable->columnCount, 0));

    while (true)
    {
        unordered_multimap<int, vector<int>> table1Records;
        for (int i = 0; i < parsedQuery.joinBufferSize - 2; i++)
        {
            vector<int> row = cursor1.getNextRowOfCurPage();
            while (!row.empty())
            {
                table1Records.insert({row[joinFirstColumnIndex], row});
                row = cursor1.getNextRowOfCurPage();
            }
            if (!table1.getNextPage(&cursor1))
                break;
        }
        if (table1Records.empty())
            break;

        Cursor cursor2 = table2.getCursor();
        while (true)
        {
            vector<int> row = cursor2.getNextRowOfCurPage();
            while (!row.empty())
            {
                auto it = table1Records.equal_range(row[joinSecondColumnIndex]);
                for (auto itr = it.first; itr != it.second; itr++)
                {
                    vector<int> resultantRow = itr->second;
                    if (isTable1Outside)
                    {
                        resultantRow = itr->second;
                        resultantRow.insert(resultantRow.end(), row.begin(), row.end());
                    }
                    else
                    {
                        resultantRow = row;
                        resultantRow.insert(resultantRow.end(), itr->second.begin(), itr->second.end());
                    }

                    resultantRows[rowCounter++] = resultantRow;
                    if (rowCounter == resultantTable->maxRowsPerBlock)
                    {
                        bufferManager.writePage(resultantTable->tableName, resultantTable->blockCount, resultantRows, rowCounter);
                        resultantTable->rowCount += rowCounter;
                        resultantTable->blockCount++;
                        resultantTable->rowsPerBlockCount.emplace_back(rowCounter);
                        rowCounter = 0;
                    }
                    // resultantTable->writeRow<int>(resultantRow);
                }
                row = cursor2.getNextRowOfCurPage();
            }
            if (!table2.getNextPage(&cursor2))
                break;
        }
    }
    if (rowCounter)
    {
        bufferManager.writePage(resultantTable->tableName, resultantTable->blockCount, resultantRows, rowCounter);
        resultantTable->rowCount += rowCounter;
        resultantTable->blockCount++;
        resultantTable->rowsPerBlockCount.emplace_back(rowCounter);
        rowCounter = 0;
    }

    if (resultantTable->rowCount)
        tableCatalogue.insertTable(resultantTable);
    else
    {
        resultantTable->unload();
        delete resultantTable;
        cout << "Empty Table" << endl;
    }
    return;
}

int hashFunction(int val, int M)
{
    logger.log("hashFunction");

    long long int x = val;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return (int)(x % M);
}

vector<int> partition(Table *table, string columnName, int M)
{
    logger.log("partition");
    Cursor cursor = table->getCursor();
    vector<int> row = cursor.getNext();

    int columnIndex = table->getColumnIndex(columnName);

    vector<vector<vector<int>>> buckets(M, vector<vector<int>>(table->maxRowsPerBlock, vector<int>(table->columnCount, 0)));
    vector<int> rowCounter(M, 0);
    vector<int> blockCount(M, 0);
    vector<vector<int>> numberOfPagesInEachBucket(M);

    while (!row.empty())
    {
        int bucket = hashFunction(row[columnIndex], M);

        buckets[bucket][rowCounter[bucket]++] = row;

        if (rowCounter[bucket] == table->maxRowsPerBlock)
        {
            numberOfPagesInEachBucket[bucket].push_back(rowCounter[bucket]);
            bufferManager.writePage(table->tableName + "_HashBucket_" + to_string(bucket), blockCount[bucket], buckets[bucket], rowCounter[bucket]);
            blockCount[bucket]++;
            rowCounter[bucket] = 0;
        }

        row = cursor.getNext();
    }

    for (int bucket = 0; bucket < M; bucket++)
    {
        if (rowCounter[bucket])
        {
            numberOfPagesInEachBucket[bucket].push_back(rowCounter[bucket]);
            bufferManager.writePage(table->tableName + "_HashBucket_" + to_string(bucket), blockCount[bucket], buckets[bucket], rowCounter[bucket]);
            blockCount[bucket]++;
            rowCounter[bucket] = 0;
        }
    }

    table->isHashed = true;
    table->numberOfPagesInEachBucket = numberOfPagesInEachBucket;

    return blockCount;
}

void executePartitionHashJoin()
{
    logger.log("executePartitionHashJoin");

    Table *table1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table *table2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);
    Table *resultantTable = createJoinTable(table1, table2);

    bool isTable1Outside = true;
    if (table1->blockCount > table2->blockCount)
    {
        isTable1Outside = false;
        swap(table1, table2);
        swap(parsedQuery.joinFirstColumnName, parsedQuery.joinSecondColumnName);
    }

    int M = parsedQuery.joinBufferSize - 1;

    vector<int> firstPartitionBlockCount = partition(table1, parsedQuery.joinFirstColumnName, M);
    vector<int> secondPartitionBlockCount = partition(table2, parsedQuery.joinSecondColumnName, M);

    int joinFirstColumnIndex = table1->getColumnIndex(parsedQuery.joinFirstColumnName);
    int joinSecondColumnIndex = table2->getColumnIndex(parsedQuery.joinSecondColumnName);

    int rowCounter = 0;
    vector<vector<int>> resultantRows(resultantTable->maxRowsPerBlock, vector<int>(resultantTable->columnCount, 0));

    for (int bucket = 0; bucket < M; bucket++)
    {
        for (int block1 = 0; block1 < firstPartitionBlockCount[bucket]; block1 += M)
        {
            unordered_multimap<int, vector<int>> table1Records;

            for (int i = 0; i < min(M, firstPartitionBlockCount[bucket] - block1); i++)
            {
                Cursor cursor1(table1->tableName + "_HashBucket_" + to_string(bucket), block1 + i);

                vector<int> row = cursor1.getNextRowOfCurPage();
                while (!row.empty())
                {
                    table1Records.insert({row[joinFirstColumnIndex], row});
                    row = cursor1.getNextRowOfCurPage();
                }
            }

            for (int block2 = 0; block2 < secondPartitionBlockCount[bucket]; block2++)
            {
                Cursor cursor2(table2->tableName + "_HashBucket_" + to_string(bucket), block2);

                vector<int> row = cursor2.getNextRowOfCurPage();

                while (!row.empty())
                {
                    auto it = table1Records.equal_range(row[joinSecondColumnIndex]);
                    for (auto itr = it.first; itr != it.second; itr++)
                    {
                        vector<int> resultantRow = itr->second;
                        if (isTable1Outside)
                        {
                            resultantRow = itr->second;
                            resultantRow.insert(resultantRow.end(), row.begin(), row.end());
                        }
                        else
                        {
                            resultantRow = row;
                            resultantRow.insert(resultantRow.end(), itr->second.begin(), itr->second.end());
                        }

                        resultantRows[rowCounter++] = resultantRow;
                        if (rowCounter == resultantTable->maxRowsPerBlock)
                        {
                            bufferManager.writePage(resultantTable->tableName, resultantTable->blockCount, resultantRows, rowCounter);
                            resultantTable->rowCount += rowCounter;
                            resultantTable->blockCount++;
                            resultantTable->rowsPerBlockCount.emplace_back(rowCounter);
                            rowCounter = 0;
                        }
                        // resultantTable->writeRow<int>(resultantRow);
                    }
                    row = cursor2.getNextRowOfCurPage();
                }
            }
        }
    }

    if (rowCounter)
    {
        bufferManager.writePage(resultantTable->tableName, resultantTable->blockCount, resultantRows, rowCounter);
        resultantTable->rowCount += rowCounter;
        resultantTable->blockCount++;
        resultantTable->rowsPerBlockCount.emplace_back(rowCounter);
        rowCounter = 0;
    }

    if (resultantTable->rowCount)
        tableCatalogue.insertTable(resultantTable);
    else
    {
        resultantTable->unload();
        delete resultantTable;
        cout << "Empty Table" << endl;
    }
    return;
}

void executeJOIN()
{
    logger.log("executeJOIN");

    if (parsedQuery.joinAlgorithm == NESTED)
    {
        executeNestedJoin();
    }
    else
    {
        executePartitionHashJoin();
    }

    cout << "Number of Block accesses for READ: " << READ_BLOCK_ACCESS_COUNT << endl;
    cout << "Number of Block accesses for WRITE: " << WRITE_BLOCK_ACCESS_COUNT << endl;

    return;
}
