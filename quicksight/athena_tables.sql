-- Athena tables for Francesco's QuickSight dashboard — proj1102 (Ralph's account 148557232117).
--
-- Registers the two CSVs uploaded by deploy.sh as queryable tables. Run from the
-- Athena query editor, or let deploy.sh run it automatically.
--
-- `timestamp` is a reserved word in Athena — it must stay backticked everywhere.
--
-- Database proj1102_db already exists (created 2026-05-06). We deliberately do
-- NOT issue CREATE DATABASE here: Athena calls the glue:CreateDatabase API even
-- with IF NOT EXISTS, and the ralph-comprehend user isn't granted that action.
-- If running against a fresh account, create the database in the console first.

-- ---------------------------------------------------------------------------
-- actuals — real prices (the blue line on the chart)
-- source: sagemaker/input/<TICKER>.parquet  ->  quicksight/actuals.csv
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS proj1102_db.actuals;
CREATE EXTERNAL TABLE proj1102_db.actuals (
  item_id         string,
  `timestamp`     string,
  target_value    double,
  sentiment_score double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://proj1102-ralph-forecast-148557232117/quicksight/actuals/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ---------------------------------------------------------------------------
-- forecast — DeepAR p10/p50/p90 (the orange line + shaded band)
-- source: sagemaker/output/<TICKER>_forecast.csv  ->  quicksight/forecast.csv
-- supersedes the old ARIMA `predictions` table — that dataset can be deleted.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS proj1102_db.forecast;
CREATE EXTERNAL TABLE proj1102_db.forecast (
  item_id     string,
  `timestamp` string,
  p10         double,
  p50         double,
  p90         double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://proj1102-ralph-forecast-148557232117/quicksight/forecast/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ---------------------------------------------------------------------------
-- daily_actuals — real daily closes (the blue line on the strategic chart)
-- source: s3://proj1102-data/daily/<TICKER>.parquet  ->  quicksight/daily-actuals.csv
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS proj1102_db.daily_actuals;
CREATE EXTERNAL TABLE proj1102_db.daily_actuals (
  item_id      string,
  `timestamp`  string,
  target_value double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://proj1102-ralph-forecast-148557232117/quicksight/daily-actuals/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ---------------------------------------------------------------------------
-- daily_forecast — 7-day-ahead p10/p50/p90 (strategic outlook band)
-- source: s3://proj1102-data/daily-forecast-output/<TICKER>/  ->  quicksight/daily-forecast.csv
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS proj1102_db.daily_forecast;
CREATE EXTERNAL TABLE proj1102_db.daily_forecast (
  item_id     string,
  `timestamp` string,
  p10         double,
  p50         double,
  p90         double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://proj1102-ralph-forecast-148557232117/quicksight/daily-forecast/'
TBLPROPERTIES ('skip.header.line.count'='1');
