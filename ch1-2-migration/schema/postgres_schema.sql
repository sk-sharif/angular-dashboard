-------------------------------------------------
---  Foreign tables from ClickHouse database  ---
-------------------------------------------------
DROP FOREIGN TABLE addotnet.ad_event_view;
CREATE
FOREIGN  TABLE
    addotnet.ad_event_view
(
    event_year                               int,
    event_month                              int8,
    event_date                               date,
    event_hour                               int8,
    event_minute                             int8,
    event_timestamp                          text,
    search_timestamp                         text,
    click_timestamp                          text,
    action_timestamp                         text,

    advertiser_lid                           bigint,
    advertiser_hid                           bigint,
    campaign_id                              bigint,
    adgroup_lid                              bigint,
    adgroup_hid                              bigint,
    provider_account_lid                     bigint,
    provider_account_hid                     bigint,
    target_id                                bigint,
    creative_id                              bigint,
    feed_advertiser_id                       bigint,
    faa_id1                                  text,
    faa_id2                                  text,
    faa_id3                                  text,
    affiliate_account_lid                    bigint,
    affiliate_account_hid                    bigint,
    traffic_source_lid                       bigint,
    traffic_source_hid                       bigint,

    adgroup_type                             text,
    uuid                                     text,
    uuid_click                               text,
    is_media_buy                             int8,
    browser_family                           text,
    browser_version                          text,
    os_family                                text,
    os_version                               text,
    device_name                              text,
    is_mobile                                int8,
    search_ip                                text,
    search_user_agent                        text,
    client_ip                                text,
    user_agent                               text,
    referer                                  text,
    fallback_url                             text,
    mark_url                                 text,
    auto_redirect_next_hop_url               text,
    publisher_referrer_domain                text,
    country_code                             int,
    zip_code                                 text,
    metro_code                               int,
    region_code                              int,
    reason_for_unpaid                        text,
    findology_internal                       int8,
    viewed_text                              text,
    target_keyword                           text,  -- epc.keyword
    search_keyword                           text,  -- please use epc.query_keyword as we may not get it from rb

    requests                                 bigint,
    ad_returns                               bigint,
    ad_returned                              text,
    search_net_bid_price                     float,
    search_gross_bid_price                   float,

    bid_modifier_multiplier                  float,
    bid_modifier_details                     text,
    cpa_goal                                 float,
    cpa_goal_runtime                         float,
    raw_clicks                               bigint,
    paid_clicks                              bigint,
    pub_payout                               float, -- net bid
    revenue                                  float, --gross bid


    eventpixel_id                            bigint,
    eventpixel_name                          text,
    eventpixel_type                          text,
    dollars_worth                            float,
    actions_worth                            float,
    event_fires_count                        bigint,
    eventpixel_margin                        float,
    eventpixel_calculated_dollars_worth      float,

    feed_request_timed_out                   int8,
    feed_response_latency_ms                 bigint,
    feed_returns                             bigint,
    feed_returned                            text,

    channel                                  int,
    cookie                                   text,
    data_size                                bigint,
    device_id                                text,
    domain_name                              text,
    highest_external_adlisting_id            text,
    highest_internal_ron_adgroup_key         text,
    highest_internal_ron_adlisting_id        text,
    highest_internal_ron_advertiser_key      text,
    highest_internal_ron_bid                 float,
    highest_internal_ron_net_bid             float,
    highest_internal_targeted_adgroup_key    text,
    highest_internal_targeted_adlisting_id   text,
    highest_internal_targeted_advertiser_key text,
    highest_internal_targeted_bid            float,
    highest_internal_targeted_net_bid        float,
    http_type                                text,
    internal_ron_adgroup_keys                text,
    internal_targeted_adgroup_keys           text,
    is_next_bidder_repeated_search           int8,
    latency                                  bigint,
    max_external_bid                         float,
    max_external_net_bid                     float,
    max_internal_bid                         float,
    max_internal_net_bid                     float,
    next_bidder_repeated_search_num          bigint,
    search_node_identifier                   text,
    num_external_ads                         int,
    num_internal_ads                         int,
    num_ron_ads                              int,
    partner_id                               text,
    predicted_searchiq_method                text,
    predicted_searchiq_user_id               text,
    protocol                                 text,
    pub_user_id                              text,
    publisher_referrer                       text,
    publisher_referrer_hostname              text,
    quality_bucket_key                       text,
    raw_keyword                              text,
    reason_not_served                        text,
    remote_requester                         text,
    remote_system                            text,
    remote_user                              text,
    server_request_port                      int,
    status_code                              int,
    time_stamp                               text,
    top_bid_feed_advertiser_id               bigint,
    top_external_bid_keyword                 text,
    top_internal_bid_keyword                 text,
    top_internal_bid_type                    text,
    traffic_provider_key                     text,
    url                                      text,
    url_parameters_json                      text,
    user_ip                                  text,
    x_forwarded_for                          text,

    able_to_set_cookie                       int8,
    anonymized_traffic                       int8,
    category_key                             text,
    click_date                               bigint,
    epc_created_on                           bigint,
    mc_created_on                            bigint,
    feed_advertiser_advertiser_id            text,
    feed_advertiser_click_url                text,
    feed_advertiser_display_url              text,
    gross_bid                                float,
    is_expanded_query                        int8,
    masked_click                             int8,
    net_bid                                  float,
    network_type                             text,
    new_user                                 int8,
    next_hop_url                             text,
    persistent_user_id_cookie                text,
    rank                                     int,
    referrer_hostname                        text,
    referring_url                            text,
    request_port                             int,
    said_category                            text,
    said_tier                                text,
    screen_height                            int,
    screen_width                             int,
    epc_node_identifier                      text,
    mc_node_identifier                       text,
    search_referrer_hostname                 text,
    searchid                                 text,
    searchiquserid                           text,
    sp_adgroup                               text,
    sp_category                              text,
    sp_feed_bid                              float,
    sp_target                                text,
    viewable_status                          text,
    viewed_url                               text,
    window_height                            int,
    window_position_left                     int,
    window_position_top                      int,
    window_width                             int,

    action_date                              text,
    action_epoch                             bigint,
    additional_get_parameters                text,
    batch                                    int8,
    benchmark_keyword                        text,
    listing_id                               text,
    matched_keyword                          text,
    action_node_identifier                   text,
    received_epoch                           bigint,
    sp_query_keyword                         text,
    t_get_parameter                          text,
    target_expanded_query_keyword            text,
    --    target_type                              text,
    aid                                      text,
    pub_classification                       text,
    adgroups_filtered                        text,

    sid                                      bigint,
    said                                     text,
    raw_said                                 text,
    dt                                       text,
    hour_id                                  int8,
    event_type                               text,
    event_fires                              bigint,
    ias_check_timedout                       bigint,
    final_redirect_expired                   bigint,
    missing_masked_click                     bigint,
    ip_mismatch_check                        bigint,
    unique_user_cookie_check                 bigint,
    time_delay_check                         bigint,
    ias_fails_check                          bigint,
    window_location                          bigint,
    window_size                              bigint,
    user_agent_mismatch_check                bigint,
    referrer_hostname_mismatch_check         bigint,
    cookie_tainting                          bigint,
    no_flash                                 bigint,
    unable_to_set_cookie                     bigint,
    global_bot_filter                        bigint,
    direct_search_filtered                   bigint,
    adgroup_name                             text,
    adgroup_status                           text,
    daily_revenue_caps                       float,
    throttling_policy_lid                    bigint,
    throttling_policy_hid                    bigint,
    throttling_policy_percentage             float,
    advertiser_name                          text,
    advertiser_status                        text,
    affiliate_account_name                   text,
    campaign_name                            text,
    campaign_status                          text,
    creative_name                            text,
    creative_description                     text,
    creative_title                           text,
    country_name                             text,
    dma_name                                 text,
    state_name                               text,
    state_short_name                         text,
    feed_advertiser_name                     text,
    feed_advertiser_status                   text,
    provider_account_name                    text,
    provider_account_status                  text,
    target_name                              text,
    target_viewed_text                       text,
    target_type                              text,
    traffic_source_name                      text,
    quality_bucket_name                      text,
    io_cap_id                                bigint,
    io_autostart                             int8,
    io_start_date                            text,
    io_end_date                              text,
    io_revenue_cap                           float,
    io_elapsed_day                           int,
    io_total_day                             int,
    source_daily_clicks_caps                 float,
    source_daily_revenue_caps                float,
    source_daily_sid_clicks_caps             float,
    source_daily_sid_revenue_caps            float,
    source_daily_sid_said_clicks_caps        float,
    source_daily_sid_said_revenue_caps       float,
    sid_said                                 text,
    publisher_category                       text,
    mark_url_hostname                        text,
    is_sid_media_buy                         int8,
    impressions                              bigint,
    advertiser_daily_revenue_caps           float,
    advertiser_monthly_revenue_caps         float,
    adgroup_daily_revenue_caps              float,
    publisher_referrer_first_significant_domain text
) SERVER clickhouse_svr;


CREATE
FOREIGN  TABLE addotnet.viglink_min_bid_report
(
event_date date,
advertiser_name text,
advertiser_lid bigint,
advertiser_hid bigint,
adgroup_name text,
said text,
pub_name text,
bid_floor text,
search_net_bid_price float,
total_ad_returns bigint,
ad_returns_above_floor bigint,
ad_returns_below_floor bigint,
raw_clicks bigint,
paid_clicks bigint,
total_revenue bigint,
total_actions_worth bigint
) SERVER clickhouse_svr;
CREATE
FOREIGN TABLE
    addotnet.ad_event_daily_view
(
    event_date                     date,
    advertiser_lid                 bigint,
    advertiser_hid                 bigint,
    campaign_id                    bigint,
    adgroup_lid                    bigint,
    adgroup_hid                    bigint,
    provider_account_lid           bigint,
    provider_account_hid           bigint,
    target_id                      bigint,
    feed_advertiser_id             bigint,
    affiliate_account_lid          bigint,
    affiliate_account_hid          bigint,

    requests                       bigint,
    ad_returns                     bigint,


    raw_clicks                     bigint,
    paid_clicks                    bigint,
    pub_payout                     float, -- net bid
    revenue                        float, --gross bid


    dollars_worth                  float,
    actions_worth                  float,
    event_fires_count              bigint,


    sid                            bigint,
    said                           text,
    adgroup_name                   text,
    advertiser_name                text,
    advertiser_status              text,
    affiliate_account_name         text,
    campaign_name                  text,
    feed_advertiser_name           text,
    provider_account_name          text,
    target_name                    text,
    updated                        bigint,
    version                        int,
    impressions                    int,
    paid_clicks_diff               int,
    revenue_diff                   float,
    pub_payout_diff                float,
    feed_generated_revenue_diff    float,
    feed_generated_pub_payout_diff float,
    mb_clicks                      int,
    mb_cost                        float,
    notes                          text,
    io_autostart                   int,
    io_start_date                  text,
    io_end_date                    text,
    io_revenue_cap                 float,
    traffic_source_name            text,
    quality_bucket_name            text


) SERVER clickhouse_svr;

DROP FOREIGN TABLE addotnet.ad_event_daily_adjustment;
CREATE
FOREIGN TABLE addotnet.ad_event_daily_adjustment
    (
        event_date date,
        event_month_id int,
        event_month_name text,
        event_year int,
        advertiser_lid bigint,
        advertiser_hid bigint,
        campaign_id bigint,
        adgroup_lid bigint,
        adgroup_hid bigint,
        provider_account_lid bigint,
        provider_account_hid bigint,
        feed_advertiser_id bigint,
        affiliate_account_lid bigint,
        affiliate_account_hid bigint,
        sid bigint,
        said text,
        requests bigint,
        ad_returns bigint,
        raw_clicks bigint,
        paid_clicks bigint,
        pub_payout float,
        revenue float,
        dollars_worth float,
        actions_worth float,
        event_fires_count bigint,
        adgroup_name text,
        advertiser_name text,
        advertiser_status text,
        affiliate_account_name text,
        is_media_buy int,
        campaign_name text,
        feed_advertiser_name text,
        feed_advertiser_status text,
        provider_account_name text,
        provider_account_status text,
        io_cap_id bigint,
        io_autostart int8,
        io_start_date date,
        io_end_date date,
        io_revenue_cap float,
        io_elapsed_day int,
        io_total_day int,
        traffic_source_lid bigint,
        traffic_source_hid bigint,
        traffic_source_name text,
        quality_bucket_name text,
        source_daily_clicks_caps float,
        source_daily_revenue_caps float,
        source_daily_sid_clicks_caps float,
        source_daily_sid_revenue_caps float,
        source_daily_sid_said_clicks_caps float,
        source_daily_sid_said_revenue_caps float,
        impressions int,
        paid_clicks_diff int,
        revenue_diff float,
        pub_payout_diff float,
        feed_generated_revenue_diff float,
        feed_generated_pub_payout_diff float,
        mb_clicks int,
        mb_cost float,
        mb_dollars_worth float,
        click_cost float,
        purchased_clicks bigint,
        today_revenue float,
        yesterday_revenue float,
        last_7_completed_days_revenue float,
        advertiser_budget_amount float,
        month_id int,
        publisher_category text,
        advertiser_daily_revenue_caps float,
        advertiser_monthly_revenue_caps float,
        cpa_goal float,
        adgroup_daily_revenue_caps float
    )
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.affiliate_account_dim
(
    affiliate_account_id bigint,
    name text,
    is_media_buy int8
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.adgroup_dim
(
    adgroup_id bigint,
    adgroup_name text,
    adgroup_status text,
    daily_revenue_caps text,
    throttling_policy_lid bigint,
    throttling_policy_hid bigint,
    throttling_policy_percentage float
)
SERVER clickhouse_svr;

DROP FOREIGN TABLE addotnet.advertiser_dim;
CREATE
FOREIGN TABLE addotnet.advertiser_dim
(
    advertiser_id bigint,
    advertiser_name text,
    advertiser_status text,
    daily_revenue_caps text,
    monthly_revenue_caps text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.campaign_dim
(
    id bigint,
    name text,
    status text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.creative_dim
(
    id bigint,
    name text,
    description text,
    title text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.country_dim
(
    code bigint,
    name text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.feed_advertiser_dim
(
    id bigint,
    name text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.io_cap_dim
(
    event_date date,
    advertiser_lid bigint,
    io_cap_id bigint,
    io_autostart int8,
    io_start_date date,
_date date,
    io_revenue_cap float,
    io_elapsed_day int,
    io_total_day int
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.provider_account_dim
(
    provider_account_id bigint,
    name text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.target_dim
(
    id bigint,
    keyword text,
    viewed_text text,
    dtype text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.traffic_source_dim
(
    sid bigint,
    lid bigint,
    hid bigint,
    name text,
    quality_bucket_name text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.regular_adjustment
(
    dt date,
    affiliateaccount_lid bigint,
    affiliateaccount_hid bigint,
    sid bigint,
    said text,
    provideraccount_lid bigint,
    provideraccount_hid bigint,
    feed_id bigint,
    advertiser_lid bigint,
    advertiser_hid bigint,
    campaign_id bigint,
    adgroup_lid bigint,
    adgroup_hid bigint,
    impressions int,
    paid_clicks_diff int,
    revenue_diff float,
    pub_payout_diff float,
    feed_generated_revenue_diff float,
    feed_generated_pub_payout_diff float,
    mb_clicks int,
    mb_cost float
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.media_buyer_adjustment
(
    sid bigint,
    mb_clicks int,
    mb_cost float,
    mb_dollars_worth float
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.feed_advertiser_adjustment
(
    said text,
    impressions int,
    paid_clicks_diff int,
    revenue_diff float,
    pub_payout_diff float,
    feed_generated_revenue_diff float,
    feed_generated_pub_payout_diff float
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.adv_camp_adg_dim
(
    advertiser_lid bigint,
    advertiser_hid bigint,
    advertiser_name text,
    advertiser_user_lid bigint,
    advertiser_user_hid bigint,
    campaign_id bigint,
    campaign_name text,
    adgroup_lid bigint,
    adgroup_hid bigint,
    adgroup_name text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.publisher_source_dim
(
    publisher_lid bigint,
    publisher_hid bigint,
    publisher_name text,
    publisher_user_lid bigint,
    publisher_user_hid bigint,
    source_lid bigint,
    source_hid bigint,
    source_name text,
    sid bigint
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.quality_bucket_dim
(
    quality_bucket_lid bigint,
    quality_bucket_hid bigint,
    quality_bucket_name text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.throttling_policy_dim
(
    throttling_policy_lid bigint,
    throttling_policy_hid bigint,
    throttling_policy_percentage float
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.provider_feed_dim
(
    provider_account_lid bigint,
    provider_account_hid bigint,
    provider_account_name text,
    provider_account_status text,
    feed_advertiser_id bigint,
    feed_advertiser_name text,
    feed_advertiser_status text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.pixel_event_dim
(
    pixe_event_id bigint,
    pixe_event_type text,
    pixe_event_name text,
    pixe_event_status text,
    adgroup_name text,
    adgroup_lid bigint,
    adgroup_hid bigint
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.designated_market_area_dim
(
    dma_code int,
    dma_name text,
    state_name text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.adgroup_revenue_cap
(
    adgroup_lid bigint,
    adgroup_hid bigint,
    source_daily_clicks_caps text,
    source_daily_revenue_caps text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.adgroup_sid_revenue_cap
(
    adgroup_lid bigint,
    adgroup_hid bigint,
    sid text,
    source_daily_sid_clicks_caps text,
    source_daily_sid_revenue_caps text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.adgroup_sid_said_revenue_cap
(
    adgroup_lid bigint,
    adgroup_hid bigint,
    sid text,
    said text,
    source_daily_sid_said_clicks_caps text,
    source_daily_sid_said_revenue_caps text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.sid_said_dim
(
    sid_said text,
    sid text,
    said text
)
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.advertiser_budget_dim
(
    advertiser_lid bigint,
    advertiser_hid bigint,
    provider_account_lid bigint,
    provider_account_hid bigint,
    month_id int,
    advertiser_name text,
    budget_amount float
)
SERVER clickhouse_svr;

----------------------------------------------------
---  Materialized view from ClickHouse database  ---
----------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS addotnet.adv_camp_adg
AS
SELECT *
FROM addotnet.adv_camp_adg_dim
WITH
NO DATA;
REFRESH MATERIALIZED VIEW addotnet.adv_camp_adg;
CREATE
UNIQUE INDEX adv_camp_adg_index ON addotnet.adv_camp_adg(advertiser_lid, advertiser_hid, advertiser_user_lid, advertiser_user_hid, campaign_id, adgroup_lid, adgroup_hid);

CREATE MATERIALIZED VIEW IF NOT EXISTS addotnet.publisher_source
AS
SELECT *
FROM addotnet.publisher_source_dim
WITH
NO DATA;
REFRESH MATERIALIZED VIEW addotnet.publisher_source;
CREATE
UNIQUE INDEX publisher_source_index ON addotnet.publisher_source(publisher_lid, publisher_hid, publisher_user_lid, publisher_user_hid, source_lid, source_hid, sid);

CREATE MATERIALIZED VIEW IF NOT EXISTS addotnet.provider_feed
AS
SELECT *
FROM addotnet.provider_feed_dim
WITH
NO DATA;
REFRESH MATERIALIZED VIEW addotnet.provider_feed;
CREATE
UNIQUE INDEX provider_feed_index ON addotnet.provider_feed(provider_account_lid, provider_account_hid, feed_advertiser_id);



--
-- PostgreSQL database dump
--

-- Dumped from database version 11.3
-- Dumped by pg_dump version 11.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: executive_view; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.executive_view AS
SELECT ad_event_daily_adjustment.event_date                    AS thedate,
       concat(ad_event_daily_adjustment.advertiser_lid, '~',
              ad_event_daily_adjustment.advertiser_hid)        AS advertiserkey,
       ad_event_daily_adjustment.advertiser_name               AS advertisername,
       'Advertiser'::text AS advertisertype,
       concat(ad_event_daily_adjustment.adgroup_lid, '~',
              ad_event_daily_adjustment.adgroup_hid)           AS adgroupfeedkey,
       ad_event_daily_adjustment.adgroup_name                  AS adgroupfeedname,
       'Adgroup'::text AS adgroupfeedtype,
       concat(ad_event_daily_adjustment.affiliate_account_lid, '~',
              ad_event_daily_adjustment.affiliate_account_hid) AS publisherkey,
       ad_event_daily_adjustment.affiliate_account_name        AS publishername,
       CASE
           WHEN (ad_event_daily_adjustment.is_media_buy = 1) THEN 'Media Buy'::text
           ELSE 'XML'::text
           END                                                 AS publishertype,
       ad_event_daily_adjustment.sid,
       sum(ad_event_daily_adjustment.revenue)                  AS revenue,
       sum(
               CASE
                   WHEN (ad_event_daily_adjustment.is_media_buy = 1) THEN ad_event_daily_adjustment.mb_cost
                   ELSE (ad_event_daily_adjustment.revenue - ad_event_daily_adjustment.revenue_diff)
                   END)                                        AS payout,
       'SEARCH'::text AS producttype
FROM addotnet.ad_event_daily_adjustment
GROUP BY ad_event_daily_adjustment.event_date, ad_event_daily_adjustment.advertiser_name,
         ad_event_daily_adjustment.advertiser_lid, ad_event_daily_adjustment.advertiser_hid,
         ad_event_daily_adjustment.adgroup_hid, ad_event_daily_adjustment.adgroup_lid,
         ad_event_daily_adjustment.affiliate_account_lid, ad_event_daily_adjustment.affiliate_account_hid,
         ad_event_daily_adjustment.adgroup_name, ad_event_daily_adjustment.affiliate_account_name,
         ad_event_daily_adjustment.sid, ad_event_daily_adjustment.is_media_buy
UNION
SELECT ad_event_daily_adjustment.event_date                    AS thedate,
       concat(ad_event_daily_adjustment.provider_account_lid, '~',
              ad_event_daily_adjustment.provider_account_hid)  AS advertiserkey,
       ad_event_daily_adjustment.provider_account_name         AS advertisername,
       'External Advertiser'::text AS advertisertype,
       (ad_event_daily_adjustment.feed_advertiser_id)::character varying AS adgroupfeedkey,
       ad_event_daily_adjustment.feed_advertiser_name          AS adgroupfeedname,
       'Feed'::text AS adgroupfeedtype,
       concat(ad_event_daily_adjustment.affiliate_account_lid, '~',
              ad_event_daily_adjustment.affiliate_account_hid) AS publisherkey,
       ad_event_daily_adjustment.affiliate_account_name        AS publishername,
       CASE
           WHEN (ad_event_daily_adjustment.is_media_buy = 1) THEN 'Media Buy'::text
           ELSE 'XML'::text
           END                                                 AS publishertype,
       ad_event_daily_adjustment.sid,
       sum(ad_event_daily_adjustment.revenue)                  AS revenue,
       sum(
               CASE
                   WHEN (ad_event_daily_adjustment.is_media_buy = 1) THEN ad_event_daily_adjustment.mb_cost
                   ELSE ad_event_daily_adjustment.revenue
                   END)                                        AS payout,
       'SEARCH'::text AS producttype
FROM addotnet.ad_event_daily_adjustment
WHERE (ad_event_daily_adjustment.feed_advertiser_id <> 72)
GROUP BY ad_event_daily_adjustment.event_date, ad_event_daily_adjustment.provider_account_name,
         ad_event_daily_adjustment.provider_account_lid, ad_event_daily_adjustment.provider_account_hid,
         ad_event_daily_adjustment.feed_advertiser_id, ad_event_daily_adjustment.feed_advertiser_name,
         ad_event_daily_adjustment.affiliate_account_lid, ad_event_daily_adjustment.affiliate_account_hid,
         ad_event_daily_adjustment.affiliate_account_name, ad_event_daily_adjustment.sid,
         ad_event_daily_adjustment.is_media_buy;


ALTER TABLE public.executive_view OWNER TO postgres;

DROP FOREIGN TABLE addotnet.ad_event_daily_adjustment_budget;
CREATE
FOREIGN TABLE addotnet.ad_event_daily_adjustment_budget
    (
        event_date date,
        event_month_id int,
        event_month_name text,
        event_year int,
        advertiser_lid bigint,
        advertiser_hid bigint,
        campaign_id bigint,
        adgroup_lid bigint,
        adgroup_hid bigint,
        provider_account_lid bigint,
        provider_account_hid bigint,
        feed_advertiser_id bigint,
        affiliate_account_lid bigint,
        affiliate_account_hid bigint,
        sid bigint,
        said text,
        requests bigint,
        ad_returns bigint,
        raw_clicks bigint,
        paid_clicks bigint,
        pub_payout float,
        revenue float,
        dollars_worth float,
        actions_worth float,
        event_fires_count bigint,
        adgroup_name text,
        advertiser_name text,
        advertiser_status text,
        affiliate_account_name text,
        is_media_buy int,
        campaign_name text,
        feed_advertiser_name text,
        feed_advertiser_status text,
        provider_account_name text,
        provider_account_status text,
        io_cap_id bigint,
        io_autostart int8,
        io_start_date date,
        io_end_date date,
        io_revenue_cap float,
        io_elapsed_day int,
        io_total_day int,
        traffic_source_lid bigint,
        traffic_source_hid bigint,
        traffic_source_name text,
        quality_bucket_name text,
        source_daily_clicks_caps float,
        source_daily_revenue_caps float,
        source_daily_sid_clicks_caps float,
        source_daily_sid_revenue_caps float,
        source_daily_sid_said_clicks_caps float,
        source_daily_sid_said_revenue_caps float,
        impressions int,
        paid_clicks_diff int,
        revenue_diff float,
        pub_payout_diff float,
        feed_generated_revenue_diff float,
        feed_generated_pub_payout_diff float,
        mb_clicks int,
        mb_cost float,
        mb_dollars_worth float,
        click_cost float,
        purchased_clicks bigint,
        today_revenue float,
        yesterday_revenue float,
        last_7_completed_days_revenue float,
        advertiser_budget_amount float,
        month_id int,
        publisher_category text,
        advertiser_daily_revenue_caps float,
        advertiser_monthly_revenue_caps float,
        cpa_goal float,
        adgroup_daily_revenue_caps float
    )
SERVER clickhouse_svr;

CREATE
FOREIGN TABLE addotnet.facebook_stats
(
    dt                   date,
    event_date           date,
    sid                  int,
    provider_id          text,
    mb_name              text,
    mb_campaign_id       text,
    mb_adgroup_id        bigint,
    adgroup_lid          bigint,
    adgroup_hid          bigint,
    feed_advertiser_id   int,
    mb_clicks            int,
    mb_cost              float,
    mb_dollars_worth     float,
    mb_ad_id             text,
    mb_ad_name           text,
    mb_ad_link           text,
    mb_website_purchases float
)
SERVER clickhouse_svr;


DROP FOREIGN TABLE IF EXISTS addotnet.bidwise_sid_mb_discrepancy_category;
CREATE
FOREIGN TABLE
    addotnet.bidwise_sid_mb_discrepancy_category
(
    event_date    date,
    said          text,
    advertiser_name          text,
    referrer_domain text,
    cat_id        bigint,
    category_details        text,
    provider_id   text,
    ssaid         text,
    actual_cost   float,
    actual_clicks bigint,
    requests      bigint,
    raw_clicks    bigint,
    paid_clicks   bigint,
    revenue       float,
    actions_worth float,
    cpa           float
)
SERVER clickhouse_svr;

DROP FOREIGN TABLE IF EXISTS addotnet.io_caps_and_budget_view;

CREATE
FOREIGN  TABLE addotnet.io_caps_and_budget_view
(
    advertiser_name                      text,
    advertiser_lid                       bigint,
    advertiser_hid                       bigint,
    advertiser_budget                    float,
    budget_revenue                       float,
    yesterday_revenue                    float,
    today_revenue                        float,
    io_caps_revenue                      float,
    io_caps_revenue_diff                 float,
    budget_last_7_completed_days_revenue float,
    io_revenue_cap                       float,
    io_total_day                         bigint,
    io_elapsed_day                       bigint,
    io_start_date                        date,
    io_end_date                          date,
    io_caps_revenue_excluding_today      float,
    io_caps_last_7_completed_days_revenue    float
) SERVER clickhouse_svr;


DROP FOREIGN TABLE IF EXISTS addotnet.io_caps_and_budget_view_v1;

CREATE
FOREIGN  TABLE addotnet.io_caps_and_budget_view_v1
(
    advertiser_name                      text,
    advertiser_lid                       bigint,
    advertiser_hid                       bigint,
    advertiser_budget                    float,
    budget_revenue                       float,
    yesterday_revenue                    float,
    today_revenue                        float,
    io_caps_revenue                      float,
    io_caps_revenue_diff                 float,
    budget_last_7_completed_days_revenue float,
    io_revenue_cap                       float,
    io_total_day                         bigint,
    io_elapsed_day                       bigint,
    io_start_date                        date,
    io_end_date                          date,
    io_caps_revenue_excluding_today      float,
    io_caps_last_7_completed_days_revenue    float
) SERVER clickhouse_svr;


DROP FOREIGN TABLE IF EXISTS addotnet.io_caps_report_view;

CREATE
FOREIGN  TABLE addotnet.io_caps_report_view
(
    advertiser_name               text,
    advertiser_lid                bigint,
    advertiser_hid                bigint,
    io_revenue_cap                float,
    io_total_day                  bigint,
    io_elapsed_day                bigint,
    io_start_date                 date,
    io_end_date                   date,
    yesterday_revenue             float,
    today_revenue                 float,
    revenue                       float,
    revenue_diff                  float,
    revenue_excluding_today       float,
    last_7_completed_days_revenue float
)SERVER clickhouse_svr;

DROP FOREIGN TABLE IF EXISTS addotnet.adgroup_sid_cap_view;

CREATE
FOREIGN  TABLE addotnet.adgroup_sid_cap_view (
event_date           date,
 adgroup_name         text,
 traffic_source_name  text,
 sid                  bigint,
 click_cap            float,
 revenue_cap          float,
 today_clicks         bigint,
 yesterday_clicks     bigint,
 today_revenue        float,
 yesterday_revenue    float,
 click_cap_percent    float,
 revenue_cap_percent  float ) SERVER clickhouse_svr;


DROP FOREIGN TABLE IF EXISTS addotnet.adgroup_denied_traffic_providers_view;

CREATE
FOREIGN  TABLE addotnet.adgroup_denied_traffic_providers_view (
sid               bigint,
 provider_id        text,
 sid_said text,
 adgroup_name       text,
 adgroup_status     text,
 campaign_status    text,
 advertiser_status  text,
 advertiser_name    text,
 publisher_name     text,
last_90_days_revenue bigint) SERVER clickhouse_svr;


DROP FOREIGN TABLE IF EXISTS addotnet.intel_ad_event_view;

CREATE
FOREIGN  TABLE addotnet.intel_ad_event_view (
    event_year                               int,
    event_month                              int8,
    event_date                               date,
    event_hour                               int8,
    event_minute                             int8,
    event_timestamp                          text,
    search_timestamp                         text,
    click_timestamp                          text,
    action_timestamp                         text,
    advertiser_lid                           bigint,
    advertiser_hid                           bigint,
    campaign_id                              bigint,
    adgroup_lid                              bigint,
    adgroup_hid                              bigint,
    provider_account_lid                     bigint,
    provider_account_hid                     bigint,
    target_id                                bigint,
    creative_id                              bigint,
    feed_advertiser_id                       bigint,
    faa_id1                                  text,
    faa_id2                                  text,
    faa_id3                                  text,
    affiliate_account_lid                    bigint,
    affiliate_account_hid                    bigint,
    traffic_source_lid                       bigint,
    traffic_source_hid                       bigint,
    adgroup_type                             text,
    uuid                                     text,
    uuid_click                               text,
    is_media_buy                             int8,
    browser_family                           text,
    browser_version                          text,
    os_family                                text,
    os_version                               text,
    device_name                              text,
    is_mobile                                int8,
    search_ip                                text,
    search_user_agent                        text,
    client_ip                                text,
    user_agent                               text,
    referer                                  text,
    fallback_url                             text,
    mark_url                                 text,
    auto_redirect_next_hop_url               text,
    publisher_referrer_domain                text,
    country_code                             int,
    zip_code                                 text,
    metro_code                               int,
    region_code                              int,
    reason_for_unpaid                        text,
    findology_internal                       int8,
    viewed_text                              text,
    target_keyword                           text,
    search_keyword                           text,
    requests                                 bigint,
    ad_returns                               bigint,
    ad_returned                              text,
    search_net_bid_price                     float,
    search_gross_bid_price                   float,
    bid_modifier_multiplier                  float,
    bid_modifier_details                     text,
    cpa_goal                                 float,
    raw_clicks                               bigint,
    paid_clicks                              bigint,
    pub_payout                               float,
    revenue                                  float,
    eventpixel_id                            bigint,
    eventpixel_name                          text,
    eventpixel_type                          text,
    dollars_worth                            float,
    actions_worth                            float,
    event_fires_count                        bigint,
    eventpixel_margin                        float,
    eventpixel_calculated_dollars_worth      float,
    feed_request_timed_out                   int8,
    feed_response_latency_ms                 bigint,
    feed_returns                             bigint,
    feed_returned                            text,
    channel                                  int,
    cookie                                   text,
    data_size                                bigint,
    device_id                                text,
    domain_name                              text,
    highest_external_adlisting_id            text,
    highest_internal_ron_adgroup_key         text,
    highest_internal_ron_adlisting_id        text,
    highest_internal_ron_advertiser_key      text,
    highest_internal_ron_bid                 float,
    highest_internal_ron_net_bid             float,
    highest_internal_targeted_adgroup_key    text,
    highest_internal_targeted_adlisting_id   text,
    highest_internal_targeted_advertiser_key text,
    highest_internal_targeted_bid            float,
    highest_internal_targeted_net_bid        float,
    http_type                                text,
    internal_ron_adgroup_keys                text,
    internal_targeted_adgroup_keys           text,
    is_next_bidder_repeated_search           int8,
    latency                                  bigint,
    max_external_bid                         float,
    max_external_net_bid                     float,
    max_internal_bid                         float,
    max_internal_net_bid                     float,
    next_bidder_repeated_search_num          bigint,
    search_node_identifier                   text,
    num_external_ads                         int,
    num_internal_ads                         int,
    num_ron_ads                              int,
    partner_id                               text,
    predicted_searchiq_method                text,
    predicted_searchiq_user_id               text,
    protocol                                 text,
    pub_user_id                              text,
    publisher_referrer                       text,
    publisher_referrer_hostname              text,
    quality_bucket_key                       text,
    raw_keyword                              text,
    reason_not_served                        text,
    remote_requester                         text,
    remote_system                            text,
    remote_user                              text,
    server_request_port                      int,
    status_code                              int,
    time_stamp                               text,
    top_bid_feed_advertiser_id               bigint,
    top_external_bid_keyword                 text,
    top_internal_bid_keyword                 text,
    top_internal_bid_type                    text,
    traffic_provider_key                     text,
    url                                      text,
    url_parameters_json                      text,
    user_ip                                  text,
    x_forwarded_for                          text,
    able_to_set_cookie                       int8,
    anonymized_traffic                       int8,
    category_key                             text,
    click_date                               bigint,
    epc_created_on                           bigint,
    mc_created_on                            bigint,
    feed_advertiser_advertiser_id            text,
    feed_advertiser_click_url                text,
    feed_advertiser_display_url              text,
    gross_bid                                float,
    is_expanded_query                        int8,
    masked_click                             int8,
    net_bid                                  float,
    network_type                             text,
    new_user                                 int8,
    next_hop_url                             text,
    persistent_user_id_cookie                text,
    rank                                     int,
    referrer_hostname                        text,
    referring_url                            text,
    request_port                             int,
    said_category                            text,
    said_tier                                text,
    screen_height                            int,
    screen_width                             int,
    epc_node_identifier                      text,
    mc_node_identifier                       text,
    search_referrer_hostname                 text,
    searchid                                 text,
    searchiquserid                           text,
    sp_adgroup                               text,
    sp_category                              text,
    sp_feed_bid                              float,
    sp_target                                text,
    viewable_status                          text,
    viewed_url                               text,
    window_height                            int,
    window_position_left                     int,
    window_position_top                      int,
    window_width                             int,
    action_date                              text,
    action_epoch                             bigint,
    additional_get_parameters                text,
    batch                                    int8,
    benchmark_keyword                        text,
    listing_id                               text,
    matched_keyword                          text,
    action_node_identifier                   text,
    received_epoch                           bigint,
    sp_query_keyword                         text,
    t_get_parameter                          text,
    target_expanded_query_keyword            text,
    sid                                      bigint,
    said                                     text,
    raw_said                                 text,
    dt                                       text,
    hour_id                                  int8,
    event_type                               text,
    event_fires                              bigint,
    ias_check_timedout                       bigint,
    final_redirect_expired                   bigint,
    missing_masked_click                     bigint,
    ip_mismatch_check                        bigint,
    unique_user_cookie_check                 bigint,
    time_delay_check                         bigint,
    ias_fails_check                          bigint,
    window_location                          bigint,
    window_size                              bigint,
    user_agent_mismatch_check                bigint,
    referrer_hostname_mismatch_check         bigint,
    cookie_tainting                          bigint,
    no_flash                                 bigint,
    unable_to_set_cookie                     bigint,
    adgroup_name                             text,
    adgroup_status                           text,
    daily_revenue_caps                       float,
    throttling_policy_lid                    bigint,
    throttling_policy_hid                    bigint,
    throttling_policy_percentage             float,
    advertiser_name                          text,
    advertiser_status                        text,
    affiliate_account_name                   text,
    campaign_name                            text,
    campaign_status                          text,
    creative_name                            text,
    creative_description                     text,
    creative_title                           text,
    country_name                             text,
    dma_name                                 text,
    state_name                               text,
    state_short_name                         text,
    feed_advertiser_name                     text,
    feed_advertiser_status                   text,
    provider_account_name                    text,
    provider_account_status                  text,
    target_name                              text,
    target_viewed_text                       text,
    target_type                              text,
    traffic_source_name                      text,
    quality_bucket_name                      text,
    io_cap_id                                bigint,
    io_autostart                             int8,
    io_start_date                            text,
    io_end_date                              text,
    io_revenue_cap                           float,
    io_elapsed_day                           int,
    io_total_day                             int,
    source_daily_clicks_caps                 float,
    source_daily_revenue_caps                float,
    source_daily_sid_clicks_caps             float,
    source_daily_sid_revenue_caps            float,
    source_daily_sid_said_clicks_caps        float,
    source_daily_sid_said_revenue_caps       float,
    sid_said                                 text,
    publisher_category                       text,
    mark_url_hostname                        text,
    is_sid_media_buy                         int8,
    impressions                              bigint,
    advertiser_daily_revenue_caps           float,
    advertiser_monthly_revenue_caps         float,
    adgroup_daily_revenue_caps              float,
    pci_aw               float,
    scroll_rate_aw       float,
    visits_aw       float) SERVER clickhouse_svr;

DROP FOREIGN TABLE IF EXISTS addotnet.new_sources_view;

create FOREIGN table addotnet.new_sources_view
(
    sid                    bigint,
    said                   text,
    affiliate_account_name text,
    traffic_source_name    text,
    advertiser_name        text,
    advertiser_lid         bigint,
    advertiser_hid         bigint,
    min_date               date,
    requests               bigint,
    ad_returns             bigint,
    raw_clicks             bigint,
    paid_clicks            bigint,
    revenue                float,
    pub_payout             float,
    actions_worth          float,
    mb_clicks              bigint
)SERVER clickhouse_svr;

DROP FOREIGN TABLE IF EXISTS addotnet.bidwise_sid_mb_discrepancy_v2;

CREATE FOREIGN TABLE addotnet.bidwise_sid_mb_discrepancy_v2
(
    event_date      date,
    said            text,
    referrer_domain text,
    provider_id     text,
    ssaid           text,
    actual_cost     float ,
    actual_clicks   bigint,
    requests        bigint,
    raw_clicks      bigint,
    paid_clicks     bigint,
    revenue         float,
    actions_worth   float,
    cpa             float
) SERVER clickhouse_svr;