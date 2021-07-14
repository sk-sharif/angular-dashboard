CREATE TABLE addotnet.amazon_attribution_fact
(
    `dt`                                      Date,
    `batch_id`                                String,
    `client_name`                             Nullable(String),
    `profile_id`                              Nullable(Int64),
    `advertiser_id`                           Nullable(String),
    `campaignId`                              Nullable(String),
    `adGroupId`                               Nullable(String),
    `creativeId`                              Nullable(String),
    `advertiserName`                          Nullable(String),
    `publisher`                               Nullable(String),
    `attributedAddToCartClicks14d`            Nullable(Int64),
    `attributedDetailPageViewsClicks14d`      Nullable(Int64),
    `attributedPurchases14d`                  Nullable(Int64),
    `attributedTotalAddToCartClicks14d`       Nullable(Int64),
    `attributedTotalPurchases14d`             Nullable(Int64),
    `totalUnitsSold14d`                       Nullable(Int64),
    `unitsSold14d`                            Nullable(Int64),
    `clickthroughs`                           Nullable(Int64),
    `attributedTotalDetailPageViewsClicks14d` Nullable(Int64),
    `attributedSales14d`                      Nullable(Float64),
    `totalAttributedSales14d`                 Nullable(Float64)
)
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/amazon_attribution_fact_1/{shard}',
             '{replica}')
        PARTITION BY (batch_id)
        PRIMARY KEY (dt, batch_id)
        ORDER BY (dt, batch_id)
        SETTINGS index_granularity = 8192;

/*insert into addotnet.amazon_attribution_fact_new
select *
from addotnet.amazon_attribution_fact
where CAST(batch_id as Int64) > 1620968408934;
*/

CREATE TABLE addotnet.amazon_attribution_fact_increment
(
    `dt`                  Date,
    `batch_id`            String,
    `profile_id`          Nullable(Int64),
    `creativeId`          Nullable(String),
    `addtocart_click_inc` Nullable(Int64),
    `purchase_sales_inc`  Nullable(Float64)
)
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/amazon_attribution_fact_increment/{shard}',
             '{replica}')
        PARTITION BY (dt, batch_id)
        PRIMARY KEY (dt, batch_id)
        ORDER BY (dt, batch_id)
        SETTINGS index_granularity = 8192;


CREATE TABLE addotnet.amazon_attribution_fb_pixel
(
    `dt`         Date,
    `batch_id`   String,
    `CLIENT_IP`  String,
    `FBCLID`     Nullable(String),
    `UA`         Nullable(String),
    `SALES`      Nullable(Float64),
    `EVENTTIME`  Nullable(Int64),
    `EVENT_TYPE` String,
    `PROFILE_ID` Nullable(Int64),
    `COUNTRY`    Nullable(String)
)
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/amazon_attribution_fb_pixel/{shard}',
             '{replica}')
        PARTITION BY (dt, batch_id)
        PRIMARY KEY (dt, batch_id)
        ORDER BY (dt, batch_id)
        SETTINGS index_granularity = 8192;