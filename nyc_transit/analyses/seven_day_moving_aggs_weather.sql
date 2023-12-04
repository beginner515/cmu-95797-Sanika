SELECT
  DATE,
  CAST(PRCP AS DOUBLE) AS PRCP,
  CAST(SNOW AS DOUBLE) AS SNOW,
  MIN(CAST(PRCP AS DOUBLE)) OVER w AS min_prcp_7day,
  MAX(CAST(PRCP AS DOUBLE)) OVER w AS max_prcp_7day,
  AVG(CAST(PRCP AS DOUBLE)) OVER w AS avg_prcp_7day,
  SUM(CAST(PRCP AS DOUBLE)) OVER w AS sum_prcp_7day,
  MIN(CAST(SNOW AS DOUBLE)) OVER w AS min_snow_7day,
  MAX(CAST(SNOW AS DOUBLE)) OVER w AS max_snow_7day,
  AVG(CAST(SNOW AS DOUBLE)) OVER w AS avg_snow_7day,
  SUM(CAST(SNOW AS DOUBLE)) OVER w AS sum_snow_7day
FROM central_park_weather
WINDOW w AS (
  ORDER BY CAST(DATE AS DATE) 
  RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING
)
ORDER BY CAST(DATE AS DATE);