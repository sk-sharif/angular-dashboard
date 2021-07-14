-- command to load publisher_category table
-- cat publisher_cat.tsv | clickhouse-client --query="INSERT INTO addotnet.publisher_category FORMAT TSVWithNames";
CREATE TABLE
    addotnet.publisher_category
(
    sid                  UInt64,
    `publisher_category` String,
    traffic_source_name  String
)
    ENGINE = MergeTree PRIMARY KEY
        (
            sid
            )
        ORDER BY
            (
                sid
                )
        SETTINGS index_granularity = 8192;

-- below are tables in clickhouse that we convert to dictionaries

CREATE VIEW addotnet.provider_dict
            (`provider_account_lid` Int64, `provider_account_hid` Int64, `provider_account_name` String) AS
SELECT DISTINCT provider_account_lid, provider_account_hid, provider_account_name
FROM addotnet.provider_feed_dim;


DROP DICTIONARY IF EXISTS addotnet.affiliate_account_dim;
CREATE DICTIONARY addotnet.affiliate_account_dim
(
    affiliate_account_id UInt64,
    name                 String,
    is_media_buy         UInt8
)PRIMARY KEY affiliate_account_id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'affiliate_account_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

DROP DICTIONARY IF EXISTS addotnet.adgroup_dim;
CREATE DICTIONARY addotnet.adgroup_dim
(
    adgroup_id                   UInt64,
    adgroup_name                 String,
    adgroup_status               String,
    daily_revenue_caps           String,
    throttling_policy_lid        Int64,
    throttling_policy_hid        Int64,
    throttling_policy_percentage Float64,
    cpa_goal                     Float64,
    default_landing_page_url String
)PRIMARY KEY adgroup_id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'adgroup_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

DROP DICTIONARY IF EXISTS addotnet.advertiser_dim;
CREATE DICTIONARY addotnet.advertiser_dim
(
    advertiser_id     UInt64,
    advertiser_name   String,
    advertiser_status String,
    daily_revenue_caps  String,
    monthly_revenue_caps String
)PRIMARY KEY advertiser_id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'advertiser_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());


DROP DICTIONARY IF EXISTS addotnet.campaign_dim;
CREATE DICTIONARY addotnet.campaign_dim
(
    id     UInt64,
    name   String,
    status String
)PRIMARY KEY id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'campaign_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());


DROP DICTIONARY IF EXISTS addotnet.creative_dim;
CREATE DICTIONARY addotnet.creative_dim
(
    id          UInt64,
    name        String,
    description String,
    title       String
)PRIMARY KEY id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'creative_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());


DROP DICTIONARY IF EXISTS addotnet.country_dim;
CREATE DICTIONARY addotnet.country_dim
(
    code UInt64,
    name String,
    abbr String
)PRIMARY KEY code
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'country_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());


DROP DICTIONARY IF EXISTS addotnet.feed_advertiser_dim;
CREATE DICTIONARY addotnet.feed_advertiser_dim
(
    id   UInt64,
    name String
)PRIMARY KEY id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'feed_advertiser_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

DROP DICTIONARY IF EXISTS addotnet.io_cap_dim;
CREATE DICTIONARY addotnet.io_cap_dim
(
    event_date     Date,
    advertiser_lid Int64,
    io_cap_id      Int64,
    io_autostart   UInt8,
    io_start_date  Date,
    io_end_date    Date,
    io_revenue_cap Float64,
    io_elapsed_day Int32,
    io_total_day   Int32
)PRIMARY KEY event_date,advertiser_lid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'io_cap_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());

DROP DICTIONARY IF EXISTS addotnet.io_cap_future_dim;
CREATE DICTIONARY addotnet.io_cap_future_dim
(
    event_date     Date,
    advertiser_lid Int64,
    io_cap_id      Int64,
    io_autostart   UInt8,
    io_start_date  Date,
    io_end_date    Date,
    io_revenue_cap Float64,
    io_elapsed_day Int32,
    io_total_day   Int32
)PRIMARY KEY event_date,advertiser_lid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'io_cap_future_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());

DROP DICTIONARY IF EXISTS addotnet.provider_account_dim;
CREATE DICTIONARY addotnet.provider_account_dim
(
    provider_account_id UInt64,
    name                String
)PRIMARY KEY provider_account_id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'provider_account_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

DROP DICTIONARY IF EXISTS addotnet.target_dim;
CREATE DICTIONARY addotnet.target_dim
(
    id          UInt64,
    keyword     String,
    viewed_text String,
    dtype       String,
    landing_page_url String
)PRIMARY KEY id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'target_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());


DROP DICTIONARY IF EXISTS addotnet.traffic_source_dim;
CREATE DICTIONARY addotnet.traffic_source_dim
(
    sid                 UInt64,
    lid                 Int64,
    hid                 Int64,
    name                String,
    quality_bucket_name String
)PRIMARY KEY sid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'traffic_source_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

DROP DICTIONARY IF EXISTS addotnet.regular_adjustment;
CREATE DICTIONARY addotnet.regular_adjustment
(
    dt                             Date,
    affiliateaccount_lid           Int64,
    affiliateaccount_hid           Int64,
    sid                            Int64,
    said                           String,
    provideraccount_lid            Int64,
    provideraccount_hid            Int64,
    feed_id                        Int64,
    advertiser_lid                 Int64,
    advertiser_hid                 Int64,
    campaign_id                    Int64,
    adgroup_lid                    Int64,
    adgroup_hid                    Int64,
    impressions                    Int32,
    paid_clicks_diff               Int64,
    revenue_diff                   Float64,
    pub_payout_diff                Float64,
    feed_generated_revenue_diff    Float64,
    feed_generated_pub_payout_diff Float64,
    mb_clicks                      Int32,
    mb_cost                        Float64
)PRIMARY KEY dt,affiliateaccount_lid,affiliateaccount_hid,sid,said,provideraccount_lid,provideraccount_hid,feed_id,advertiser_lid,advertiser_hid,campaign_id,adgroup_lid,adgroup_hid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'regular_adjustment' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.media_buyer_adjustment;
CREATE DICTIONARY addotnet.media_buyer_adjustment
(
    dt                   Date,
    affiliateaccount_lid Int64,
    affiliateaccount_hid Int64,
    sid                  Int64,
    provideraccount_lid  Int64,
    provideraccount_hid  Int64,
    feed_id              Int64,
    advertiser_lid       Int64,
    advertiser_hid       Int64,
    campaign_id          Int64,
    adgroup_lid          Int64,
    adgroup_hid          Int64,
    mb_clicks            Int32,
    mb_cost              Float64,
    mb_dollars_worth     Float64
)PRIMARY KEY dt,affiliateaccount_lid,affiliateaccount_hid,sid,provideraccount_lid,provideraccount_hid,feed_id,advertiser_lid,advertiser_hid,campaign_id,adgroup_lid,adgroup_hid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'media_buyer_adjustment' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());

DROP DICTIONARY IF EXISTS addotnet.bidwise_adjustment;
CREATE DICTIONARY addotnet.bidwise_adjustment
(
    dt                   Date,
    affiliateaccount_lid Int64,
    affiliateaccount_hid Int64,
    sid                  Int64,
    said                 String,
    provideraccount_lid  Int64,
    provideraccount_hid  Int64,
    feed_id              Int64,
    advertiser_lid       Int64,
    advertiser_hid       Int64,
    campaign_id          Int64,
    adgroup_lid          Int64,
    adgroup_hid          Int64,
    mb_clicks            Int32,
    mb_cost              Float64,
    mb_dollars_worth     Float64
)PRIMARY KEY dt,affiliateaccount_lid,affiliateaccount_hid,sid,said,provideraccount_lid,provideraccount_hid,feed_id,advertiser_lid,advertiser_hid,campaign_id,adgroup_lid,adgroup_hid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'bidwise_adjustment' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.feed_advertiser_adjustment;
CREATE DICTIONARY addotnet.feed_advertiser_adjustment
(
    dt                             Date,
    affiliateaccount_lid           Int64,
    affiliateaccount_hid           Int64,
    sid                            Int64,
    said                           String,
    provideraccount_lid            Int64,
    provideraccount_hid            Int64,
    feed_id                        Int64,
    advertiser_lid                 Int64,
    advertiser_hid                 Int64,
    campaign_id                    Int64,
    adgroup_lid                    Int64,
    adgroup_hid                    Int64,
    impressions                    Int32,
    paid_clicks_diff               Int64,
    revenue_diff                   Float64,
    pub_payout_diff                Float64,
    feed_generated_revenue_diff    Float64,
    feed_generated_pub_payout_diff Float64
)PRIMARY KEY dt,affiliateaccount_lid,affiliateaccount_hid,sid,said,provideraccount_lid,provideraccount_hid,feed_id,advertiser_lid,advertiser_hid,campaign_id,adgroup_lid,adgroup_hid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'feed_advertiser_adjustment' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());



DROP DICTIONARY IF EXISTS addotnet.adv_camp_adg_dim;
CREATE DICTIONARY addotnet.adv_camp_adg_dim
(
    advertiser_lid      Int64,
    advertiser_hid      Int64,
    advertiser_name     String,
    advertiser_user_lid Int64,
    advertiser_user_hid Int64,
    campaign_id         Int64,
    campaign_name       String,
    adgroup_lid         Int64,
    adgroup_hid         Int64,
    adgroup_name        String
)PRIMARY KEY advertiser_lid,advertiser_hid,advertiser_user_lid,advertiser_user_hid,campaign_id,adgroup_lid,adgroup_hid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'adv_camp_adg_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());

DROP DICTIONARY IF EXISTS addotnet.publisher_source_dim;
CREATE DICTIONARY addotnet.publisher_source_dim
(
    publisher_lid      Int64,
    publisher_hid      Int64,
    publisher_name     String,
    publisher_user_lid Int64,
    publisher_user_hid Int64,
    source_lid         Int64,
    source_hid         Int64,
    source_name        String,
    sid                Int64
)PRIMARY KEY publisher_lid,publisher_hid,publisher_user_lid,publisher_user_hid,source_lid,source_hid,sid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'publisher_source_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.quality_bucket_dim;
CREATE DICTIONARY addotnet.quality_bucket_dim
(
    quality_bucket_lid  Int64,
    quality_bucket_hid  Int64,
    quality_bucket_name String
)PRIMARY KEY quality_bucket_lid,quality_bucket_hid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'quality_bucket_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.throttling_policy_dim;
CREATE DICTIONARY addotnet.throttling_policy_dim
(
    throttling_policy_lid        Int64,
    throttling_policy_hid        Int64,
    throttling_policy_percentage Float32
)PRIMARY KEY throttling_policy_lid,throttling_policy_hid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'throttling_policy_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.provider_feed_dim;
CREATE DICTIONARY addotnet.provider_feed_dim
(
    provider_account_lid    Int64,
    provider_account_hid    Int64,
    provider_account_name   String,
    provider_account_status String,
    feed_advertiser_id      Int64,
    feed_advertiser_name    String,
    feed_advertiser_status  String
)PRIMARY KEY provider_account_lid,provider_account_hid,feed_advertiser_id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'provider_feed_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.pixel_event_dim;
CREATE DICTIONARY addotnet.pixel_event_dim
(
    pixe_event_id     Int64,
    pixe_event_type   String,
    pixe_event_name   String,
    pixe_event_status String,
    adgroup_name      String,
    adgroup_lid       Int64,
    adgroup_hid       Int64
)PRIMARY KEY pixe_event_id,adgroup_lid,adgroup_hid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'pixel_event_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.designated_market_area_dim;
CREATE DICTIONARY addotnet.designated_market_area_dim
(
    dma_code Int32,
    dma_name String
)PRIMARY KEY dma_code
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'designated_market_area_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.region_dim;
CREATE DICTIONARY addotnet.region_dim
(
    region_code      Int32,
    state_name       String,
    state_short_name String
)PRIMARY KEY region_code
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'region_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.adgroup_revenue_cap;
CREATE DICTIONARY addotnet.adgroup_revenue_cap
(
    adgroup_lid               Int64,
    adgroup_hid               Int64,
    source_daily_clicks_caps  String,
    source_daily_revenue_caps String
)PRIMARY KEY adgroup_lid,adgroup_hid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'adgroup_revenue_cap' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.adgroup_sid_revenue_cap;
CREATE DICTIONARY addotnet.adgroup_sid_revenue_cap
(
    adgroup_lid                   Int64,
    adgroup_hid                   Int64,
    sid                           String,
    source_daily_sid_clicks_caps  String,
    source_daily_sid_revenue_caps String
)PRIMARY KEY adgroup_lid,adgroup_hid,sid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'adgroup_sid_revenue_cap' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.adgroup_sid_said_revenue_cap;
CREATE DICTIONARY addotnet.adgroup_sid_said_revenue_cap
(
    adgroup_lid                        Int64,
    adgroup_hid                        Int64,
    sid                                String,
    said                               String,
    source_daily_sid_said_clicks_caps  String,
    source_daily_sid_said_revenue_caps String
)PRIMARY KEY adgroup_lid,adgroup_hid,sid,said
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'adgroup_sid_said_revenue_cap' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.advertiser_budget_dim;
CREATE DICTIONARY addotnet.advertiser_budget_dim
(
    advertiser_lid       Int64,
    advertiser_hid       Int64,
    provider_account_lid Int64,
    provider_account_hid Int64,
    month_id             Int32,
    advertiser_name      String,
    budget_amount        Float64
)PRIMARY KEY advertiser_lid,advertiser_hid,provider_account_lid,provider_account_hid,month_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'advertiser_budget' PASSWORD '' DB 'addotnet'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.provider_dim;
CREATE DICTIONARY addotnet.provider_dim
(
    provider_account_lid  Int64,
    provider_account_hid  Int64,
    provider_account_name String
)PRIMARY KEY provider_account_lid,provider_account_hid
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'provider_dict' PASSWORD '' DB 'addotnet'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.publisher_category_dim;
CREATE DICTIONARY addotnet.publisher_category_dim
(
    sid                 UInt64,
    publisher_category  String,
    traffic_source_name String
)PRIMARY KEY sid
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'publisher_category' PASSWORD '' DB 'addotnet'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());


DROP DICTIONARY IF EXISTS addotnet.facebook_dim;
CREATE DICTIONARY IF NOT EXISTS addotnet.facebook_dim
(
    event_date           Date,
    affiliate_account_lid Int64,
    affiliate_account_hid Int64,
    sid                   Int64,
    provider_account_lid  Int64,
    provider_account_hid  Int64,
    feed_advertiser_id    Int64,
    advertiser_lid        Int64,
    advertiser_hid        Int64,
    campaign_id           Int64,
    adgroup_lid           Int64,
    adgroup_hid           Int64,
    mb_clicks             Int64,
    mb_cost               Float64,
    mb_dollars_worth      Float64
)PRIMARY KEY event_date, affiliate_account_lid, affiliate_account_hid, sid, provider_account_lid, provider_account_hid, feed_advertiser_id, advertiser_lid, advertiser_hid, campaign_id, adgroup_lid, adgroup_hid
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'facebook_stats_view' PASSWORD '' DB 'addotnet'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.bidwise_category_dim;
CREATE DICTIONARY IF NOT EXISTS addotnet.bidwise_category_dim
(
    category_id UInt64,
    category String
)
PRIMARY KEY category_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'bidwise_category_view' PASSWORD '' DB 'test'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

DROP DICTIONARY IF EXISTS addotnet.directnavlink_dim;
CREATE DICTIONARY addotnet.directnavlink_dim
(
    search_id     String,
    no_ad_returned_dont_deeplink_mark_url Int8
)PRIMARY KEY search_id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'directnavlink_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.adv_campaign_geo_targeting_exclusion;
CREATE DICTIONARY addotnet.adv_campaign_geo_targeting_exclusion
(
    advertiser_lid Int64,
    advertiser_hid Int64,
    advertiser_name String,
    campaign_id Int64,
    campaign_name String,
    excluded_dma_code Int64,
    excluded_dma_name String,
    excluded_region_code Int64,
    excluded_region_name String
)PRIMARY KEY advertiser_lid, advertiser_hid, campaign_id, excluded_dma_code, excluded_region_code
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'adv_campaign_geo_targeting_exclusion' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.advertiser_by_adgroup_dim;
CREATE DICTIONARY addotnet.advertiser_by_adgroup_dim
(
    adgroup_id     UInt64,
    adgroup_name   String,
    campaign_name String,
    advertiser_name   String,
    advertiser_status String,
    campaign_status String,
    adgroup_status String,
    company_name String
)PRIMARY KEY adgroup_id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'advertiser_by_adgroup_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());


DROP DICTIONARY IF EXISTS addotnet.publisher_by_source_dim;
CREATE DICTIONARY addotnet.publisher_by_source_dim
(
    sid     UInt64,
    publisher_name   String,
    publisher_status String
)PRIMARY KEY sid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'publisher_by_source_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());



DROP DICTIONARY IF EXISTS addotnet.adgroup_denied_traffic_providers;
CREATE
DICTIONARY addotnet.adgroup_denied_traffic_providers
(
    id              UInt64,
    sid              UInt64,
    provider_id       String,
    adgroup_id      UInt64
)PRIMARY KEY id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'adgroup_denied_traffic_providers' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());


DROP DICTIONARY IF EXISTS addotnet.publisher_managers_dim;
CREATE DICTIONARY addotnet.publisher_managers_dim
(
    name     String,
    username String
)PRIMARY KEY username
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'publisher_managers_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());



DROP DICTIONARY IF EXISTS addotnet.managers_by_publisher_dim;
CREATE
DICTIONARY addotnet.managers_by_publisher_dim
(
    affiliate_account_lid              UInt64,
    publisher_managers              String
)PRIMARY KEY affiliate_account_lid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'managers_by_publisher_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());


DROP DICTIONARY IF EXISTS addotnet.propel_domain_id_mapping_dim;
CREATE DICTIONARY addotnet.propel_domain_id_mapping_dim
(
    domain_id   String,
    domain      String
)PRIMARY KEY domain_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'propel_domain_id_mapping' PASSWORD '' DB 'addotnet'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS addotnet.target_ekeyword_dim;
CREATE
DICTIONARY addotnet.target_ekeyword_dim
(
id UInt64,
keyword String,
trafficProviderKey String,
target_id UInt64,
createdBy String,
createdOn Date,
updatedBy String,
updatedOn Date,
status UInt64,
url String
)PRIMARY KEY id
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'TargetExpandedQueryKeyword' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());



CREATE VIEW addotnet.adv_camp_adg (`advertiser_lid` Int64, `advertiser_hid` Int64, `advertiser_user_lid` Int64, `advertiser_user_hid` Int64, `campaign_id` Int64, `adgroup_lid` Int64, `adgroup_hid` Int64, `advertiser_name` String, `campaign_name` String, `adgroup_name` String) AS SELECT * FROM addotnet.adv_camp_adg_dim
;
CREATE VIEW addotnet.publisher_source (`publisher_lid` Int64, `publisher_hid` Int64, `publisher_user_lid` Int64, `publisher_user_hid` Int64, `source_lid` Int64, `source_hid` Int64, `sid` Int64, `publisher_name` String, `source_name` String) AS SELECT * FROM addotnet.publisher_source_dim;


DROP DICTIONARY IF EXISTS addotnet.aid_to_advertiser_dim;
CREATE DICTIONARY addotnet.aid_to_advertiser_dim
(
    aid                 String,
    advertiser_lid      Int64,
    advertiser_hid      Int64
)PRIMARY KEY aid
SOURCE(MYSQL(HOST 'f2bizslave1.data.int.dc1.ad.net' PORT 3307 USER 'prod.rw.f2biz' TABLE 'aid_to_advertiser_dim' PASSWORD '' DB 'f2_biz'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());