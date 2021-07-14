CREATE VIEW adgroup_dim AS
SELECT abs(adg.LID)     AS adgroup_id,
       adg.name         AS adgroup_name,
       CASE
           WHEN adg.status = 1 THEN 'ACTIVE'
           WHEN adg.status = 0 THEN 'NEW'
           WHEN adg.status = 2 THEN 'PAUSED'
           WHEN adg.status = 3 THEN 'DELETED'
           WHEN adg.status = 4 THEN 'REJECTED'
           ELSE ''
           END          AS adgroup_status,
       dailyRevenueCaps AS daily_revenue_caps,
       t.LID            AS throttling_policy_lid,
       t.HID            AS throttling_policy_hid,
       t.percentage     AS throttling_policy_percentage,
       adg.ecpa         as cpa_goal,
       defaultLandingPageUrl AS default_landing_page_url
FROM Adgroup adg
         JOIN ThrottlingPolicy t
              ON adg.throttlingPolicy_LID = t.LID AND adg.throttlingPolicy_HID = t.HID;

CREATE VIEW advertiser_dim AS
SELECT abs(adv.lid) advertiser_id,
    ci.companyname advertiser_name,
    CASE
        WHEN adv.status = 0 THEN 'PENDING'
        WHEN adv.status = 1 THEN 'IN_PROGRESS'
        WHEN adv.status = 2 THEN 'ACTIVE'
        WHEN adv.status = 3 THEN 'REJECTED'
        WHEN adv.status = 4 THEN 'PAUSED'
        WHEN adv.status = 5 THEN 'SUSPENDED'
        ELSE 'CLOSED'
        END AS advertiser_status,
    dailyRevenueCaps AS daily_revenue_caps,
    monthlyRevenueCaps AS monthly_revenue_caps
FROM Advertiser adv
         JOIN ContactInfo ci
              ON adv.contactinfo_lid = ci.lid AND adv.contactinfo_hid = ci.hid;

CREATE VIEW affiliate_account_dim AS
SELECT abs(lid) affiliate_account_id, name, CAST(mediaBuy AS UNSIGNED) AS is_media_buy
FROM AffiliateAccount;

CREATE VIEW date_seq_t_view AS
select 0 i
union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9;

CREATE VIEW date_seq_v_view AS
SELECT adddate('1970-01-01', t4.i * 10000 + t3.i * 1000 + t2.i * 100 + t1.i * 10 + t0.i) AS selected_date
FROM date_seq_t_view t0, date_seq_t_view t1, date_seq_t_view t2, date_seq_t_view t3, date_seq_t_view t4;

CREATE VIEW date_seq_view AS
SELECT date(selected_date) AS event_date
FROM date_seq_v_view
WHERE selected_date between '2018-06-04' and now();

create view io_cap_end_date_filled as
select temp1.advertiser_LID,
       temp1.revenueCap,
       temp1.autoStart,
       temp1.id,
       temp1.start,
       COALESCE(temp1.end, min(temp2.start) - INTERVAL 1 day) end
FROM IOCap temp1
         LEFT JOIN IOCap temp2
                   ON temp1.advertiser_LID = temp2.advertiser_LID and temp1.id < temp2.id and temp1.end is null
group by 1, 2, 3, 4, 5;

CREATE VIEW io_cap_dim AS
SELECT date_seq_view.event_date                              AS event_date,
       COALESCE(advertiser_LID, 0)                           AS advertiser_lid,
       id                                                    AS io_cap_id,
       CAST(autoStart AS UNSIGNED)                           AS io_autostart,
       start                                                 AS io_start_date,
       end                                                   AS io_end_date,
       revenueCap                                            AS io_revenue_cap,
       CASE
           WHEN DATEDIFF(COALESCE(IOCap.end, NOW()), IOCap.start) < DATEDIFF(NOW(), IOCap.start)
               THEN DATEDIFF(COALESCE(IOCap.end, NOW()), IOCap.start) + 1
           ELSE DATEDIFF(NOW(), IOCap.start) END                io_elapsed_day,
       DATEDIFF(COALESCE(IOCap.end, NOW()), IOCap.start) + 1 AS io_total_day
FROM io_cap_end_date_filled IOCap, date_seq_view
WHERE date_seq_view.event_date >= IOCap.start AND date_seq_view.event_date <= COALESCE(IOCap.end, DATE(NOW()));

CREATE VIEW provider_account_dim AS
SELECT abs(lid) provider_account_id, name
FROM ProviderAccount;

CREATE VIEW traffic_source_dim AS
SELECT tp.sid  AS sid,
       ts.lid  AS lid,
       ts.hid  AS hid,
       ts.name AS name,
       qb.name AS quality_bucket_name
FROM TrafficProvider tp
         LEFT JOIN TrafficSource ts ON tp.source_LID = ts.LID AND tp.source_HID = ts.HID
         LEFT JOIN QualityBucket qb ON tp.qualityBucket_LID = qb.LID AND tp.qualityBucket_HID = qb.HID
WHERE tp.sid NOT IN (3785246);
DROP VIEW target_dim;
CREATE VIEW target_dim AS
SELECT id, keyword, viewedText AS viewed_text, DTYPE AS dtype,
landingPageUrl as landing_page_url
FROM Target;

CREATE VIEW region_dim AS
SELECT code as region_code, name as state_name, shortName AS state_short_name
FROM Region;

CREATE VIEW country_dim AS
SELECT code, name, abbr
FROM Country;

CREATE VIEW creative_dim AS
SELECT id, name, description, title
FROM Creative;

CREATE VIEW feed_advertiser_dim as
select id, name
FROM FeedAdvertiser;


CREATE VIEW regular_adjustment AS
SELECT dt,
       COALESCE(affiliateAccount_LID, 0)   AS affiliateaccount_lid,
       COALESCE(affiliateAccount_HID, 0)   AS affiliateaccount_hid,
       COALESCE(sid, 0)                    AS sid,
       COALESCE(said, '')                  AS said,
       COALESCE(providerAccount_LID, 0)    AS provideraccount_lid,
       COALESCE(providerAccount_HID, 0)    AS provideraccount_hid,
       COALESCE(feed_id, 0)                AS feed_id,
       COALESCE(advertiser_LID, 0)         AS advertiser_lid,
       COALESCE(advertiser_HID, 0)         AS advertiser_hid,
       COALESCE(campaign_id, 0)            AS campaign_id,
       COALESCE(adgroup_LID, 0)            AS adgroup_lid,
       COALESCE(adgroup_HID, 0)            AS adgroup_hid,
       SUM(impressions)                    AS impressions,
       SUM(paid_clicks_diff)               AS paid_clicks_diff,
       SUM(revenue_diff)                   AS revenue_diff,
       SUM(pub_payout_diff)                AS pub_payout_diff,
       SUM(feed_generated_revenue_diff)    AS feed_generated_revenue_diff,
       SUM(feed_generated_pub_payout_diff) AS feed_generated_pub_payout_diff,
       SUM(mb_clicks)                      AS mb_clicks,
       SUM(mb_cost)                        AS mb_cost
FROM RevenueAdjustment
GROUP BY dt, affiliateAccount_LID, affiliateAccount_HID, sid, said, providerAccount_LID, providerAccount_HID, feed_id,
         advertiser_LID, advertiser_HID, campaign_id, adgroup_LID, adgroup_HID;

CREATE VIEW media_buyer_adjustment AS
SELECT dt,
       COALESCE(affiliateAccount_LID, 0) AS affiliateaccount_lid,
       COALESCE(affiliateAccount_HID, 0) AS affiliateaccount_hid,
       COALESCE(sid, 0)                  AS sid,
       COALESCE(providerAccount_LID, 0)  AS provideraccount_lid,
       COALESCE(providerAccount_HID, 0)  AS provideraccount_hid,
       COALESCE(feed_id, 0)              AS feed_id,
       COALESCE(advertiser_LID, 0)       AS advertiser_lid,
       COALESCE(advertiser_HID, 0)       AS advertiser_hid,
       COALESCE(campaign_id, 0)          AS campaign_id,
       COALESCE(adgroup_LID, 0)          AS adgroup_lid,
       COALESCE(adgroup_HID, 0)          AS adgroup_hid,
       SUM(mb_clicks)                    AS mb_clicks,
       SUM(mb_cost)                      AS mb_cost,
       SUM(mb_dollars_worth)             AS mb_dollars_worth
FROM RevenueAdjustment
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;

CREATE VIEW bidwise_adjustment AS
SELECT dt,
       COALESCE(affiliateAccount_LID, 0) AS affiliateaccount_lid,
       COALESCE(affiliateAccount_HID, 0) AS affiliateaccount_hid,
       COALESCE(sid, 0)                  AS sid,
       COALESCE(LOWER(said), '')         AS said,
       -7976688271575191110              AS provideraccount_lid,
       -9082673130544935034              AS provideraccount_hid,
       72                                AS feed_id,
       COALESCE(advertiser_LID, 0)       AS advertiser_lid,
       COALESCE(advertiser_HID, 0)       AS advertiser_hid,
       COALESCE(campaign_id, 0)          AS campaign_id,
       COALESCE(adgroup_LID, 0)          AS adgroup_lid,
       COALESCE(adgroup_HID, 0)          AS adgroup_hid,
       SUM(mb_clicks)                    AS mb_clicks,
       SUM(mb_cost)                      AS mb_cost,
       SUM(mb_dollars_worth)             AS mb_dollars_worth
FROM RevenueAdjustment
WHERE sid = 11172614
  AND affiliateAccount_LID = '-8280958097931867273'
  AND affiliateAccount_HID = '7468428493293569627'
  AND said IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;

CREATE VIEW feed_advertiser_adjustment as
SELECT `RevenueAdjustment`.`dt`                                  AS `dt`,
       COALESCE(`RevenueAdjustment`.`affiliateAccount_LID`, 0)   AS `affiliateaccount_lid`,
       COALESCE(`RevenueAdjustment`.`affiliateAccount_HID`, 0)   AS `affiliateaccount_hid`,
       COALESCE(`RevenueAdjustment`.`sid`, 0)                    AS `sid`,
       COALESCE(`RevenueAdjustment`.`said`, '')                  AS `said`,
       COALESCE(`RevenueAdjustment`.`providerAccount_LID`, 0)    AS `provideraccount_lid`,
       COALESCE(`RevenueAdjustment`.`providerAccount_HID`, 0)    AS `provideraccount_hid`,
       COALESCE(`RevenueAdjustment`.`feed_id`, 0)                AS `feed_id`,
       COALESCE(`RevenueAdjustment`.`advertiser_LID`, 0)         AS `advertiser_lid`,
       COALESCE(`RevenueAdjustment`.`advertiser_HID`, 0)         AS `advertiser_hid`,
       COALESCE(`RevenueAdjustment`.`campaign_id`, 0)            AS `campaign_id`,
       COALESCE(`RevenueAdjustment`.`adgroup_LID`, 0)            AS `adgroup_lid`,
       COALESCE(`RevenueAdjustment`.`adgroup_HID`, 0)            AS `adgroup_hid`,
       SUM(`RevenueAdjustment`.`impressions`)                    AS `impressions`,
       SUM(`RevenueAdjustment`.`paid_clicks_diff`)               AS `paid_clicks_diff`,
       SUM(`RevenueAdjustment`.`revenue_diff`)                   AS `revenue_diff`,
       SUM(`RevenueAdjustment`.`pub_payout_diff`)                AS `pub_payout_diff`,
       SUM(`RevenueAdjustment`.`feed_generated_revenue_diff`)    AS `feed_generated_revenue_diff`,
       SUM(`RevenueAdjustment`.`feed_generated_pub_payout_diff`) AS `feed_generated_pub_payout_diff`
FROM `RevenueAdjustment`
WHERE adjustment_source <> 'ALA_ADJUSTMENT'
GROUP BY `RevenueAdjustment`.`dt`,
         `RevenueAdjustment`.`affiliateAccount_LID`,
         `RevenueAdjustment`.`affiliateAccount_HID`,
         `RevenueAdjustment`.`sid`,
         `RevenueAdjustment`.`said`,
         `RevenueAdjustment`.`providerAccount_LID`,
         `RevenueAdjustment`.`providerAccount_HID`,
         `RevenueAdjustment`.`feed_id`,
         `RevenueAdjustment`.`advertiser_LID`,
         `RevenueAdjustment`.`advertiser_HID`,
         `RevenueAdjustment`.`campaign_id`,
         `RevenueAdjustment`.`adgroup_LID`,
         `RevenueAdjustment`.`adgroup_HID`;

CREATE VIEW revenue_adjustment AS
SELECT dt,
       updated,
       version,
       COALESCE(affiliateAccount_LID, 0) AS affiliateaccount_lid,
       COALESCE(affiliateAccount_HID, 0) AS affiliateaccount_hid,
       COALESCE(sid, 0)                  AS sid,
       COALESCE(said, 0)                 AS said,
       COALESCE(providerAccount_LID, 0)  AS provideraccount_lid,
       COALESCE(providerAccount_HID, 0)  AS provideraccount_hid,
       COALESCE(feed_id, 0)              AS feed_id,
       COALESCE(advertiser_LID, 0)       AS advertiser_lid,
       COALESCE(advertiser_HID, 0)       AS advertiser_hid,
       COALESCE(campaign_id, 0)          AS campaign_id,
       COALESCE(adgroup_LID, 0)          AS adgroup_lid,
       COALESCE(adgroup_HID, 0)          AS adgroup_hid,
       COALESCE(target_id, 0)            AS target_id,
       COALESCE(target_viewed_text, '')  AS target_viewed_text,
       impressions,
       paid_clicks_diff,
       revenue_diff,
       pub_payout_diff,
       feed_generated_revenue_diff,
       feed_generated_pub_payout_diff,
       mb_clicks,
       mb_cost,
       notes
FROM RevenueAdjustment;

CREATE VIEW adv_camp_adg_dim AS
SELECT COALESCE(adv.LID, 0) AS advertiser_lid,
       COALESCE(adv.HID, 0) AS advertiser_hid,
       ci.companyname       AS advertiser_name,
       adv.user_LID         AS advertiser_user_lid,
       adv.user_HID         AS advertiser_user_hid,
       camp.id              AS campaign_id,
       camp.name            AS campaign_name,
       COALESCE(adg.LID, 0) AS adgroup_lid,
       COALESCE(adg.HID, 0) AS adgroup_hid,
       adg.name             AS adgroup_name
FROM Advertiser adv
         JOIN ContactInfo ci
              ON adv.contactInfo_LID = ci.LID AND adv.contactInfo_HID = ci.HID
         JOIN Campaign camp
              ON adv.LID = camp.advertiser_LID AND adv.HID = camp.advertiser_HID
         JOIN Adgroup adg
              ON camp.id = adg.campaign_id;

CREATE VIEW publisher_source_dim AS
SELECT COALESCE(affa.LID, 0)    AS publisher_lid,
       COALESCE(affa.HID, 0)    AS publisher_hid,
       affa.name                AS publisher_name,
       affau.administrators_LID AS publisher_user_lid,
       affau.administrators_HID AS publisher_user_hid,
       COALESCE(ts.LID, 0)      AS source_lid,
       COALESCE(ts.HID, 0)      AS source_hid,
       ts.name                  AS source_name,
       tp.sid                   AS sid
FROM AffiliateAccount affa
         JOIN AffiliateAccount_User affau
              ON affa.LID = affau.AffiliateAccount_LID AND affa.HID = affau.AffiliateAccount_HID
         JOIN TrafficSource ts
              ON affa.LID = ts.affiliate_LID AND affa.HID = ts.affiliate_HID
         JOIN TrafficProvider tp
              ON ts.LID = tp.source_LID AND ts.HID = tp.source_HID;

CREATE VIEW quality_bucket_dim AS
SELECT COALESCE(LID, 0) AS quality_bucket_lid,
       COALESCE(HID, 0) AS quality_bucket_hid,
       name             AS quality_bucket_name
FROM QualityBucket;

CREATE VIEW throttling_policy_dim AS
SELECT COALESCE(LID, 0) AS throttling_policy_lid,
       COALESCE(HID, 0) AS throttling_policy_hid,
       percentage       AS throttling_policy_percentage
FROM ThrottlingPolicy;

CREATE VIEW provider_feed_dim AS
SELECT COALESCE(pa.LID, 0) AS provider_account_lid,
       COALESCE(pa.HID, 0) AS provider_account_hid,
       pa.name             AS provider_account_name,
       CASE
           WHEN pa.status = 0 THEN 'IN_PROGRESS'
           WHEN pa.status = 1 THEN 'ACTIVE'
           WHEN pa.status = 2 THEN 'SUSPENDED'
           WHEN pa.status = 3 THEN 'INACTIVE'
           ELSE ''
           END             AS provider_account_status,
       COALESCE(fa.id, 0)  AS feed_advertiser_id,
       fa.name             AS feed_advertiser_name,
       CASE
           WHEN fa.status = 0 THEN 'PENDING'
           WHEN fa.status = 1 THEN 'IN_REVIEW'
           WHEN fa.status = 2 THEN 'IN_PROGRESS'
           WHEN fa.status = 3 THEN 'ACTIVE'
           WHEN fa.status = 4 THEN 'REJECTED'
           WHEN fa.status = 5 THEN 'SUSPENDED'
           ELSE ''
           END             AS feed_advertiser_status
FROM ProviderAccount pa
         JOIN FeedAdvertiser fa
              ON fa.providerAccount_HID = pa.HID AND fa.providerAccount_LID = pa.LID;

CREATE VIEW campaign_dim AS
SELECT id      AS id,
       name    AS name,
       CASE
           WHEN status = 0 THEN 'NEW'
           WHEN status = 1 THEN 'ACTIVE'
           WHEN status = 2 THEN 'PAUSED'
           WHEN status = 3 THEN 'DELETED'
           ELSE ''
           END AS status
FROM Campaign;

CREATE VIEW pixel_event_dim AS
SELECT pe.id        AS pixe_event_id,
       pe.eventType AS pixe_event_type,
       pe.name      AS pixe_event_name,
       CASE
           WHEN pe.status = 0 THEN 'ACTIVE'
           WHEN pe.status = 1 THEN 'DELETED'
           ELSE ''
           END      AS pixe_event_status,
       adg.name     AS adgroup_name,
       adg.lid      AS adgroup_lid,
       adg.hid      AS adgroup_hid
FROM PixelEvent pe
         JOIN Adgroup adg
              ON adg.lid = pe.adgroup_LID AND adg.HID = pe.adgroup_HID;

CREATE VIEW designated_market_area_dim AS
SELECT COALESCE(code, 0) AS dma_code,
       name              AS dma_name
FROM DesignatedMarketArea;

CREATE VIEW adgroup_revenue_cap AS
SELECT COALESCE(adgroup_LID, 0) AS adgroup_lid,
       COALESCE(adgroup_HID, 0) AS adgroup_hid,
       dailyClickCaps           AS source_daily_clicks_caps,
       dailyRevenueCaps         AS source_daily_revenue_caps
FROM AdgroupSubIdCap
WHERE publisherSubId IN ('*', '*-', '*-*')
  AND publisherSubId LIKE '*-*%';

CREATE VIEW adgroup_sid_revenue_cap AS
SELECT COALESCE(adgroup_LID, 0)                AS adgroup_lid,
       COALESCE(adgroup_HID, 0)                AS adgroup_hid,
       SUBSTRING_INDEX(publisherSubId, '-', 1) as sid,
       dailyClickCaps                          AS source_daily_sid_clicks_caps,
       dailyRevenueCaps                        AS source_daily_sid_revenue_caps
FROM AdgroupSubIdCap
WHERE publisherSubId NOT IN ('*', '*-', '*-*')
  AND (publisherSubId LIKE '%-*%' OR publisherSubId LIKE '%-' OR publisherSubId NOT LIKE '%-%');

CREATE VIEW adgroup_sid_said_revenue_cap AS
SELECT COALESCE(adgroup_LID, 0)                                          AS adgroup_lid,
       COALESCE(adgroup_HID, 0)                                          AS adgroup_hid,
       SUBSTRING_INDEX(publisherSubId, '-', 1)                           as sid,
       SUBSTRING_INDEX(SUBSTRING_INDEX(publisherSubId, '-', 2), '-', -1) as said,
       dailyClickCaps                                                    AS source_daily_sid_said_clicks_caps,
       dailyRevenueCaps                                                  AS source_daily_sid_said_revenue_caps
FROM AdgroupSubIdCap
WHERE publisherSubId NOT IN ('*', '*-', '*-*')
  AND publisherSubId NOT LIKE '%-*'
  AND publisherSubId LIKE '%-%';


-- for dma exclusion list

CREATE VIEW Dma_us_only
AS
select *
FROM DesignatedMarketArea
where countryCode = 840;


CREATE VIEW GeoTargeting_active_policy_dma AS
SELECT distinct c.geoTargetingPolicy_HID
from
Campaign c
JOIN Advertiser adv
ON c.advertiser_LID = adv.LID AND c.advertiser_HID = adv.HID
where adv.status = 2 and c.status = 1 and geoTargetingPolicy_HID IN (select geoTargetingPolicy_HID FROM GeoTargetingPolicy_DesignatedMarketArea)


DROP VIEW GeoTargetingPolicy_AllDma;
create view GeoTargetingPolicy_AllDma AS
select *
from GeoTargeting_active_policy_dma,Dma_us_only;


CREATE VIEW region_us_only
AS
select *
FROM Region
where countryCode = 840;


CREATE VIEW GeoTargeting_active_policy_region AS
SELECT distinct c.geoTargetingPolicy_HID
from
Campaign c
JOIN Advertiser adv
ON c.advertiser_LID = adv.LID AND c.advertiser_HID = adv.HID
where adv.status = 2 and c.status = 1 and geoTargetingPolicy_HID IN (select geoTargetingPolicy_HID FROM GeoTargetingPolicy_Region)




create view GeoTargetingPolicy_AllRegion AS
select *
from GeoTargeting_active_policy_region,region_us_only;


CREATE VIEW adv_campaign_geo_targeting_exclusion AS
select adv.LID advertiser_lid, adv.HID advertiser_hid, ci.companyname advertiser_name, c.id campaign_id, c.name campaign_name,  all_gtp_dma.code excluded_dma_code, all_gtp_dma.name excluded_dma_name, null excluded_region_code, null excluded_region_name
from
    Campaign c
        JOIN Advertiser adv
             ON c.advertiser_LID = adv.LID AND c.advertiser_HID = adv.HID
        JOIN
    ContactInfo ci
    ON
                adv.contactinfo_lid = ci.lid
            AND adv.contactinfo_hid = ci.hid
        JOIN
    GeoTargetingPolicy_AllDma all_gtp_dma
    ON c.geoTargetingPolicy_HID = all_gtp_dma.geoTargetingPolicy_HID
        LEFT JOIN
    GeoTargetingPolicy_DesignatedMarketArea gtp_dma
    ON gtp_dma.allowedDesignatedMarketAreas_code   = all_gtp_dma.code and all_gtp_dma.GeoTargetingPolicy_HID = gtp_dma.GeoTargetingPolicy_HID
where gtp_dma.allowedDesignatedMarketAreas_code is null
  and adv.status = 2 and c.status = 1
UNION ALL
select adv.LID advertiser_lid, adv.HID advertiser_hid, ci.companyname advertiser_name, c.id campaign_id, c.name campaign_name, null  excluded_dma_code, null excluded_dma_name,   all_gtp_region.code excluded_region_code, all_gtp_region.name excluded_region_name
from
    Campaign c
        JOIN Advertiser adv
             ON c.advertiser_LID = adv.LID AND c.advertiser_HID = adv.HID
        JOIN
    ContactInfo ci
    ON
                adv.contactinfo_lid = ci.lid
            AND adv.contactinfo_hid = ci.hid
        JOIN
    GeoTargetingPolicy_AllRegion all_gtp_region
    ON c.geoTargetingPolicy_HID = all_gtp_region.geoTargetingPolicy_HID
        LEFT JOIN
    GeoTargetingPolicy_Region gtp_region
    ON gtp_region.allowedRegions_code   = all_gtp_region.code and all_gtp_region.GeoTargetingPolicy_HID = gtp_region.GeoTargetingPolicy_HID
where gtp_region.allowedRegions_code is null
  and adv.status = 2 and c.status = 1
;



DROP VIEW IF EXISTS directnavlink_dim;

CREATE VIEW directnavlink_dim AS
SELECT searchId as search_id, CASE WHEN noAdReturnedDontDeeplinkMarkUrl = true THEN 1 ELSE 0 END as no_ad_returned_dont_deeplink_mark_url
from DirectNavLinkId;


CREATE VIEW advertiser_by_adgroup_dim AS
SELECT
    abs(adg.LID)     AS adgroup_id,
    adg.name as adgroup_name,
    c.name as campaign_name,
    a.name AS advertiser_name,
    CASE
        WHEN a.status = 0 THEN 'PENDING'
        WHEN a.status = 1 THEN 'IN_PROGRESS'
        WHEN a.status = 2 THEN 'ACTIVE'
        WHEN a.status = 3 THEN 'REJECTED'
        WHEN a.status = 4 THEN 'PAUSED'
        WHEN a.status = 5 THEN 'SUSPENDED'
        ELSE 'CLOSED'
        END AS advertiser_status,
    CASE
        WHEN c.status = 0 THEN 'NEW'
        WHEN c.status = 1 THEN 'ACTIVE'
        WHEN c.status = 2 THEN 'PAUSED'
        WHEN c.status = 3 THEN 'DELETED'
        ELSE ''
        END AS campaign_status,
    CASE
        WHEN adg.status = 1 THEN 'ACTIVE'
        WHEN adg.status = 0 THEN 'NEW'
        WHEN adg.status = 2 THEN 'PAUSED'
        WHEN adg.status = 3 THEN 'DELETED'
        WHEN adg.status = 4 THEN 'REJECTED'
        ELSE ''
        END          AS adgroup_status,
    ci.companyName AS company_name
FROM
    Adgroup adg
        LEFT JOIN
    Campaign c ON c.id = adg.campaign_id
        LEFT JOIN
    Advertiser a ON a.hid = c.advertiser_HID
        AND a.lid = c.advertiser_LID
        LEFT JOIN
    ContactInfo ci ON ci.LID = a.contactInfo_LID
        AND ci.HID = a.contactInfo_HID;

CREATE VIEW publisher_by_source_dim AS
SELECT
    tp.sid AS sid,
    CASE
        WHEN aa.accountStatus = 0 THEN 'PENDING'
        WHEN aa.accountStatus = 1 THEN 'IN_REVIEW'
        WHEN aa.accountStatus = 2 THEN 'ACTIVE'
        WHEN aa.accountStatus = 3 THEN 'REJECTED'
        WHEN aa.accountStatus = 4 THEN 'SUSPENDED'
        WHEN aa.accountStatus = 5 THEN 'IN_PROGRESS'
        WHEN aa.accountStatus = 6 THEN 'DELETED'
        END AS publisher_status,
    aa.name AS publisher_name
FROM
    TrafficProvider tp
        LEFT JOIN
    TrafficSource ts ON tp.source_LID = ts.LID
        AND tp.source_HID = ts.HID
        LEFT JOIN
    AffiliateAccount aa ON aa.HID = ts.affiliate_HID
        AND aa.LID = ts.affiliate_LID;


DROP VIEW IF EXISTS adgroup_denied_traffic_providers;
CREATE VIEW adgroup_denied_traffic_providers AS
select id,sid, providerId as provider_id, abs(adgroup_lid) as adgroup_id
from AdgroupDeniedTrafficProviders;


DROP VIEW IF exists publisher_managers_dim;
CREATE VIEW publisher_managers_dim as
SELECT
    u.name, u.username
FROM
    User u
        INNER JOIN
    User_PrivilegedResource upr ON upr.user_LID = u.lid  and upr.user_HID = u.hid  and u.status=1
        INNER JOIN
    PrivilegedResource pr ON pr.lid = upr.privilegedResources_LID and pr.name IN ('Publisher_Manager' , 'Unlimited_Publisher_Manager')
GROUP BY u.name , u.username;


DROP VIEW IF EXISTS managers_by_publisher_dim;
CREATE VIEW managers_by_publisher_dim AS
SELECT
    ABS(au.AffiliateAccount_LID) AS affiliate_account_lid,
    GROUP_CONCAT(CONCAT(u.name, '(', u.username, ')')) publisher_managers
FROM
    AffiliateAccount_User au
        LEFT JOIN
    User u ON u.LID = au.administrators_LID
        AND u.HID = au.administrators_HID
GROUP BY au.AffiliateAccount_LID;


drop view if exists date_seq_view1;
CREATE VIEW date_seq_view1 AS
SELECT date(selected_date) AS event_date
FROM  date_seq_v_view
WHERE selected_date between now() and DATE_ADD(now(),INTERVAL 180 DAY);

drop view if exists io_cap_end_date_filled1;
CREATE VIEW io_cap_end_date_filled1 AS
SELECT
    temp1.advertiser_LID,
    temp1.revenueCap,
    temp1.autoStart,
    temp1.id,
    temp1.start,
    COALESCE(temp1.end,
             MIN(temp2.start) - INTERVAL 1 DAY) end
FROM
    IOCap temp1
        LEFT JOIN
    IOCap temp2 ON temp1.advertiser_LID = temp2.advertiser_LID
        AND temp1.id < temp2.id
        AND temp1.end IS NULL
GROUP BY 1 , 2 , 3 , 4 , 5;

drop view if exists io_cap_future_dim;
CREATE VIEW io_cap_future_dim AS
SELECT
    date_seq_view1.event_date AS event_date,
    COALESCE(advertiser_LID, 0) AS advertiser_lid,
    id AS io_cap_id,
    CAST(autoStart AS UNSIGNED) AS io_autostart,
    start AS io_start_date,
    end AS io_end_date,
    revenueCap AS io_revenue_cap,
    CASE
        WHEN DATEDIFF(NOW(), IOCap.start) < 0 THEN 0
        WHEN DATEDIFF(COALESCE(IOCap.end, NOW()), IOCap.start) < DATEDIFF(NOW(), IOCap.start) THEN DATEDIFF(COALESCE(IOCap.end, NOW()), IOCap.start) + 1
        ELSE DATEDIFF(NOW(), IOCap.start)
        END io_elapsed_day,
    CASE
        WHEN IOCap.end is null THEN 0
        ELSE DATEDIFF(IOCap.end, IOCap.start) + 1
        END AS io_total_day
FROM
    io_cap_end_date_filled1 IOCap,
        date_seq_view1
    WHERE
        date_seq_view1.event_date >= IOCap.start
            AND date_seq_view1.event_date <= COALESCE(IOCap.end,
                DATE_ADD(NOW(), INTERVAL 180 DAY));


drop view if exists dnl_to_advertiser_dim;
CREATE VIEW dnl_to_advertiser_dim AS
SELECT
    directNavLinkId_id, a.lid, a.hid
FROM
    DirectNavLinkIdAdgroup dnladg
        LEFT JOIN
    Adgroup adg ON adg.lid = dnladg.lid
        AND adg.hid = dnladg.hid
        LEFT JOIN
    Campaign c ON c.id = adg.campaign_id
        LEFT JOIN
    Advertiser a ON a.lid = c.advertiser_LID
        AND a.hid = c.advertiser_HID
WHERE
    directNavLinkId_id IS NOT NULL
        UNION SELECT
                  directNavLinkId_id, a.lid, a.hid
              FROM
                  DirectNavLinkIdCampaign dnlc
                      LEFT JOIN
                  Campaign c ON c.id = dnlc.campaignId
                      LEFT JOIN
                  Advertiser a ON a.lid = c.advertiser_LID
                      AND a.hid = c.advertiser_HID
              WHERE
                  directNavLinkId_id IS NOT NULL
              UNION SELECT
                        directNavLinkId_id, a.lid, a.hid
                    FROM
                        DirectNavLinkIdAdvertiser dnla
                            LEFT JOIN
                        Advertiser a ON a.lid = dnla.lid AND a.hid = dnla.hid
                    WHERE
                        directNavLinkId_id IS NOT NULL;

drop view if exists aid_to_advertiser_dim;
CREATE VIEW aid_to_advertiser_dim AS
SELECT
    dnl.searchId AS aid, a.lid AS advertiser_lid,a.hid AS advertiser_hid
FROM
    dnl_to_advertiser_dim a
        LEFT JOIN
    DirectNavLinkId dnl ON a.directNavLinkId_id = dnl.id
GROUP BY aid , a.lid , a.hid;