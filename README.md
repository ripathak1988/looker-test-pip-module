# looker-test-framework-pypi-module
This repo contains code base for looker-snowflake test framework pip module.

This repo has all the common methods for Looker Testing. Test scripts will be using these methods for writing Looker-Snowflake tests.

login() - for getting Looker Authentication token for further API calls.

getQueryIdForQuerySlug(slug_id,token,base_url) - for getting query information based on the slug Id. This is an internal function. This is called by method getQueryResults(url_id, token).

getQueryResultsForQueryId(query_id,token,base_url) - for getting query results based on the query Id. This is an internal function. This is called by method getQueryResults(url_id, token).

stringToJson(text) - transforms text to Json format.

ordered(obj) - used in assertion while comparing 2 lists or objects.

snowflake_connect() - connects to Snowflake DB.

dbToJson(cur, columns) - transforms DB rows (cursor) to Json format.

getQueryResults(url_id, token) - for getting query results based on the slug Id / Url Id.

dfToJson(dataframe) - transforms data frame to Json format.
