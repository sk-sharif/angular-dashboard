CREATE EXTERNAL TABLE IF NOT EXISTS addotnet.adsbrain_yahoo_feed_report
(
  event_date string,
  search_channel int,
  keyword string,
  searches int,
  clicks int,
  amount double
)
PARTITIONED BY (dt STRING)
ROW format delimited fields terminated by ',' stored as textfile
LOCATION '/user/hive/warehouse/addotnet.db/adsbrain_yahoo_feed_report'
tblproperties('skip.header.line.count'='1');