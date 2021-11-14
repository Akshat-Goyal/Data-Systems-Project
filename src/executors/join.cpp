#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- JOIN USING <join_algorithm> <relation_name1>, <relation_name2> ON <column_name1> <bin_op> <column_name2> BUFFER <buffer_size>
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 11 || tokenizedQuery[1] != "USING" || tokenizedQuery[5] != "ON" || tokenizedQuery[9] != "BUFFER")
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinFirstRelationName = tokenizedQuery[3];
    parsedQuery.joinSecondRelationName = tokenizedQuery[4];
    parsedQuery.joinFirstColumnName = tokenizedQuery[6];
    parsedQuery.joinSecondColumnName = tokenizedQuery[8];
    parsedQuery.joinBufferSize = stoi(tokenizedQuery[10]);

    string joinAlgorithm = tokenizedQuery[2];
    if (joinAlgorithm == "NESTED")
        parsedQuery.joinAlgorithm = NESTED;
    else if (joinAlgorithm == "PARTHASH")
        parsedQuery.joinAlgorithm = PARTHASH;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    string binaryOperator = tokenizedQuery[7];
    if (binaryOperator == "<")
        parsedQuery.joinBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.joinBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.joinBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.joinBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.joinBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.joinBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    regex numeric("[+]?[0-9]+");
    string bufferSize = tokenizedQuery[10];
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

void executeJOIN()
{
    logger.log("executeJOIN");
    return;
}