CREATE TABLE external_reporting.samsungGalaxyWatch_ekw_correction
(
    `event_date`            Date,
    `uuid_click`            String,
    `adgroup_lid`           String,
    `adgroup_hid`           String,
    `is_mobile`             Nullable(Int16),
    `target_id`             Nullable(Int64),
    `viewed_text`           String,
    `corrected_viewed_text` String,
    `paid_clicks`           Nullable(Int64),
    `revenue`               Float64
) ENGINE = ReplicatedMergeTree(
           '/clickhouse/{cluster}/external_reporting/prod/tables/samsungGalaxyWatch_ekw_correction/{shard}',
           '{replica}') ORDER BY tuple() SETTINGS index_granularity = 8192;

CREATE TABLE external_reporting.kohls_category_mapping
(
    `keyword`  String,
    `category` String
) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/external_reporting/prod/tables/kohls_category_mapping/{shard}',
           '{replica}') ORDER BY tuple() SETTINGS index_granularity = 8192;

CREATE TABLE external_reporting.samsungGalaxyWatch_mapping
(
    `campaign`        String,
    `adgroup`         String,
    `keyword`         String,
    `clickserver_url` String,
    `account`         String,
    `kw_landing_page` String
) ENGINE = ReplicatedMergeTree(
           '/clickhouse/{cluster}/external_reporting/prod/tables/samsungGalaxyWatch_mapping/{shard}',
           '{replica}') ORDER BY tuple() SETTINGS index_granularity = 8192;


CREATE TABLE external_reporting.humana_mapping
(
    `campaign_name` String,
    `adgroup_name`  String,
    `keyword`       String
)
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/external_reporting/prod/tables/humana_mapping/{shard}',
             '{replica}')
        ORDER BY tuple()
        SETTINGS index_granularity = 8192;

CREATE TABLE external_reporting.linzess_20210101_20210125_stats
(
    `event_date`        Date,
    `metro_code`        Nullable(Int64),
    `dma_name`          Nullable(String),
    `device_type`       Nullable(String),
    `campaign_id`       Nullable(Int64),
    `campaign_name`     Nullable(String),
    `adgroup_lid`       Nullable(Int64),
    `adgroup_hid`       Nullable(Int64),
    `adgroup_name`      Nullable(String),
    `final_clicks`      Nullable(Int64),
    `conversions`       Nullable(Int64),
    `spend`             Nullable(Float64),
    `final_impressions` Nullable(Int64)
)
    ENGINE = ReplicatedMergeTree(
             '/clickhouse/{cluster}/external_reporting/prod/tables/linzess_20210101_20210125_stats/{shard}',
             '{replica}')
        ORDER BY tuple()
        SETTINGS index_granularity = 8192;


CREATE TABLE external_reporting.restasis_20201201_20201222_aggregated
(
    `event_date`   Date,
    `adgroup_name` Nullable(String),
    `paid_clicks`  Nullable(Int64),
    `impressions`  Nullable(Int64),
    `spend`        Nullable(Float64)
)
    ENGINE = ReplicatedMergeTree(
             '/clickhouse/{cluster}/external_reporting/prod/tables/restasis_20201201_20201222_aggregated/{shard}',
             '{replica}')
        ORDER BY tuple()
        SETTINGS index_granularity = 8192;

CREATE TABLE external_reporting.restasis_20201201_20201222_stats
(
    `event_date`        Date,
    `metro_code`        Nullable(Int64),
    `dma_name`          Nullable(String),
    `device_type`       Nullable(String),
    `campaign_id`       Nullable(Int64),
    `campaign_name`     Nullable(String),
    `adgroup_lid`       Nullable(Int64),
    `adgroup_hid`       Nullable(Int64),
    `adgroup_name`      Nullable(String),
    `final_clicks`      Nullable(Int64),
    `conversions`       Nullable(Int64),
    `spend`             Nullable(Float64),
    `final_impressions` Nullable(Int64)
)
    ENGINE = ReplicatedMergeTree(
             '/clickhouse/{cluster}/external_reporting/prod/tables/restasis_20201201_20201222_stats/{shard}',
             '{replica}')
        ORDER BY tuple()
        SETTINGS index_granularity = 8192;

CREATE TABLE external_reporting.samsungCanvasMapping
(
    `account`         String,
    `adgroup`         String,
    `keyword`         String,
    `kw_landing_page` String,
    `campaign`        String
)
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/external_reporting/prod/tables/samsungCanvasMapping/{shard}',
             '{replica}')
        ORDER BY tuple()
        SETTINGS index_granularity = 8192;


CREATE TABLE external_reporting.samsungCanvas_ekw_correction
(
    `event_date`            Date,
    `uuid_click`            String,
    `adgroup_lid`           String,
    `adgroup_hid`           String,
    `is_mobile`             Nullable(Int16),
    `target_id`             Nullable(Int64),
    `viewed_text`           String,
    `corrected_viewed_text` String,
    `paid_clicks`           Nullable(Int64),
    `revenue`               Float64
)
    ENGINE = ReplicatedMergeTree(
             '/clickhouse/{cluster}/external_reporting/prod/tables/samsungCanvas_ekw_correction/{shard}', '{replica}')
        ORDER BY tuple()
        SETTINGS index_granularity = 8192;


CREATE TABLE external_reporting.samsungGalaxyBookFlex_ekw_correction
(
    `event_date`            Date,
    `uuid_click`            String,
    `adgroup_lid`           String,
    `adgroup_hid`           String,
    `is_mobile`             Nullable(Int16),
    `target_id`             Nullable(Int64),
    `viewed_text`           String,
    `corrected_viewed_text` String,
    `paid_clicks`           Nullable(Int64),
    `revenue`               Float64
)
    ENGINE = ReplicatedMergeTree(
             '/clickhouse/{cluster}/external_reporting/prod/tables/samsungGalaxyBookFlex_ekw_correction/{shard}',
             '{replica}')
        ORDER BY tuple()
        SETTINGS index_granularity = 8192;

CREATE TABLE external_reporting.samsungGalaxyBookFlex_mapping
(
    `campaign`  String,
    `adgroup`   String,
    `keyword`   String,
    `final_url` String
)
    ENGINE = ReplicatedMergeTree(
             '/clickhouse/{cluster}/external_reporting/prod/tables/samsungGalaxyBookFlex_mapping/{shard}', '{replica}')
        ORDER BY tuple()
        SETTINGS index_granularity = 8192;
        
CREATE TABLE external_reporting.comcast_mapping 
(
 `adgroup` Nullable(String),
 `click_url` Nullable(String)
) 
  ENGINE = ReplicatedMergeTree(
      '/clickhouse/{cluster}/external_reporting/prod/tables/comcast_mapping/{shard}', '{replica}')
ORDER BY tuple() 
SETTINGS index_granularity = 8192;


CREATE TABLE external_reporting.discover_mapping 
(
    `keyword` String,
    `campaign` String,
    `label` String,
    `sourceCodes` String
) 
    ENGINE = ReplicatedMergeTree(
        '/clickhouse/{cluster}/external_reporting/prod/tables/discover_mapping/{shard}', '{replica}')
ORDER BY tuple() 
SETTINGS index_granularity = 8192;


CREATE TABLE external_reporting.samsungCanvas_20210329_20210331_static 
(
`date` Date, `engine` Nullable(String),
`account` Nullable(String),
`campaign` Nullable(String),
`adgroup` Nullable(String),
`keyword` Nullable(String),
`clicks` Nullable(Int64),
`impr` Nullable(Int64),
`cost` Nullable(Float64),
`lands` Nullable(String),
`device_type` Nullable(String),
`match_type` Nullable(String),
`kw_landing_page` Nullable(String)
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{cluster}/external_reporting/prod/tables/samsungCanvas_20210329_20210331_static/{shard}', '{replica}')
ORDER BY tuple() 
SETTINGS index_granularity = 8192;
