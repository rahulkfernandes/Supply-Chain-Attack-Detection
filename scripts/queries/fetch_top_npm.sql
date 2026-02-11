/* 
Run this query in Google BigQuery
This queries the deps_dev_v1 dataset from deps.dev to produce a list of 
the top 10000 npm packages based on dependency count (proxy for popularity).

Save the result csv in data/raw/npm_packages
*/

SELECT
  Name AS package_name,
  COUNT(*) AS dependent_count
FROM
  `bigquery-public-data.deps_dev_v1.DependentsLatest`
WHERE
  System = 'NPM'
  AND NOT STARTS_WITH(Name, '@types/')
GROUP BY
  Name
ORDER BY
  dependent_count DESC
LIMIT 10000;
