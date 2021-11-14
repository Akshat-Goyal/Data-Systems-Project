#include "global.h"

/**
 * @brief
 * SYNTAX: <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> RETURN MAX|MIN|SUM|AVG(<attribute>)
 */

bool syntacticParseGROUPBY()
{
	logger.log("syntacticParseGROUPBY");
	if (tokenizedQuery.size() != 9 || tokenizedQuery[3] != "BY" || tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "RETURN")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = GROUPBY;
	parsedQuery.groupByResultRelationName = tokenizedQuery[0];
	parsedQuery.groupByGroupingAttribute = tokenizedQuery[4];
	parsedQuery.groupByRelationName = tokenizedQuery[6];

	if (tokenizedQuery[8].length() < 6 || tokenizedQuery[8][3] != '(' || tokenizedQuery[8][tokenizedQuery[8].length() - 1] != ')' || (tokenizedQuery[8].substr(0, 3) != "MAX" && tokenizedQuery[8].substr(0, 3) != "MIN" && tokenizedQuery[8].substr(0, 3) != "SUM" && tokenizedQuery[8].substr(0, 3) != "AVG"))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.groupByAggregateOperator = tokenizedQuery[8].substr(0, 3);
	parsedQuery.groupByAggregateAttribute = tokenizedQuery[8].substr(4, tokenizedQuery[8].length() - 5);

	return true;
}

bool semanticParseGROUPBY()
{
	logger.log("semanticParseGROUPBY");

	if (tableCatalogue.isTable(parsedQuery.groupByResultRelationName))
	{
		cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
		return false;
	}

	if (!tableCatalogue.isTable(parsedQuery.groupByRelationName))
	{
		cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
		return false;
	}

	Table table = *tableCatalogue.getTable(parsedQuery.groupByRelationName);

	if (!table.isColumn(parsedQuery.groupByGroupingAttribute) || !table.isColumn(parsedQuery.groupByAggregateAttribute))
	{
		cout << "SEMANTIC ERROR: Column doesn't exist in relation";
		return false;
	}

	return true;
}

void executeGROUPBY()
{
	logger.log("executeGROUPBY");

	Table table = *tableCatalogue.getTable(parsedQuery.groupByRelationName);

	Cursor cursor = table.getCursor();
	vector<int> row = cursor.getNext();

	map<int, int> resultTable;
	map<int, int> countRowsPerGroupingAttribute;

	int groupingAttributeIndex = table.getColumnIndex(parsedQuery.groupByGroupingAttribute);
	int aggregateAttributeIndex = table.getColumnIndex(parsedQuery.groupByAggregateAttribute);

	while (!row.empty())
	{
		if (parsedQuery.groupByAggregateOperator == "MAX" || parsedQuery.groupByAggregateOperator == "MIN")
		{
			if (resultTable.find(row[groupingAttributeIndex]) == resultTable.end())
			{
				resultTable[row[groupingAttributeIndex]] = row[aggregateAttributeIndex];
			}

			if (parsedQuery.groupByAggregateOperator == "MAX")
			{
				resultTable[row[groupingAttributeIndex]] = max(resultTable[row[groupingAttributeIndex]], row[aggregateAttributeIndex]);
			}

			else
			{
				resultTable[row[groupingAttributeIndex]] = min(resultTable[row[groupingAttributeIndex]], row[aggregateAttributeIndex]);
			}
		}

		else
		{
			resultTable[row[groupingAttributeIndex]] += row[aggregateAttributeIndex];

			if (parsedQuery.groupByAggregateOperator == "AVG")
			{
				countRowsPerGroupingAttribute[row[groupingAttributeIndex]]++;
			}
		}

		row = cursor.getNext();
	}

	vector<string> columnList;
	columnList.push_back(parsedQuery.groupByGroupingAttribute);
	columnList.push_back(parsedQuery.groupByAggregateOperator + parsedQuery.groupByAggregateAttribute);
	Table *resultantTable = new Table(parsedQuery.groupByResultRelationName, columnList);

	return;
}
