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

    parsedQuery.queryType = JOIN;
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

void executeSORT()
{
    logger.log("executeSORT");
    return;
}