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
}

void executeGROUPBY()
{
	logger.log("executeGROUPBY");

	return;
}
