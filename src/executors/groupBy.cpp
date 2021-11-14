#include "global.h"

/**
 * @brief
 * SYNTAX: <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> RETURN MAX|MIN|SUM|AVG(<attribute>)
 */

bool syntacticParseGROUPBY()
{
	logger.log("syntacticParseGROUPBY");
	parsedQuery.queryType = GROUPBY;
	parsedQuery.exportRelationName = tokenizedQuery[1];
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
