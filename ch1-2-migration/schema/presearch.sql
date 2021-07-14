CREATE TABLE addotnet.presearch_page_url_tagging
(
    `page_url` String,
    `tag`      String,
    `owner`    String
) ENGINE = ReplicatedMergeTree(
           '/clickhouse/{cluster}/addotnet/prod/tables/presearch_page_url_tagging/{shard}',
           '{replica}') ORDER BY tuple() SETTINGS index_granularity = 8192;

CREATE TABLE addotnet.adunit_version
(
    `id`         String,
    `text`       String,
    `link`       String,
    `ad_unit_id` String,
    `type`       String,
    `selector`   String,
    `header`     String,
    `version`    String,
    `page_url`   String,
    `max_show`   UInt8,
    `paused`     UInt8,
    `dt`         String
) ENGINE = ReplicatedMergeTree(
           '/clickhouse/{cluster}/addotnet/prod/tables/adunit_version/{shard}',
           '{replica}') ORDER BY tuple() SETTINGS index_granularity = 8192;

CREATE VIEW addotnet.presearch_performance_etl
AS
SELECT click_stats.dt,
       click_stats.said,
       click_stats.page_url,
       COALESCE(click_stats.ad_unit_version, '') AS ad_unit_version,
       ''                                        AS ad_unit_type,
       click_stats.keyword,
       0                                         AS keyword_impressions,
       COALESCE(click_stats.clicks, 0)           AS clicks,
       COALESCE(click_stats.raw_clicks, 0)       AS raw_clicks,
       COALESCE(click_stats.paid_clicks, 0)      AS paid_clicks,
       COALESCE(uuid_click, '')                  AS uuid_click,
       ''                                        AS dnl,
       ''                                        AS query_keyword,
       0                                         AS page_clicks,
       0                                         AS num_ads
FROM (SELECT dt,
             said,
             keyword,
             page_url,
             ad_unit_version,
             clicks,
             raw_clicks,
             paid_clicks,
             uuid_click
      FROM (SELECT uuid_click, partner_id, SUM(raw_clicks) AS raw_clicks, SUM(paid_clicks) AS paid_clicks
            FROM addotnet.ad_event_view
            WHERE (sid = 11172302)
              AND (dt >= '2021-01-01')
              AND (event_type = 'click')
            GROUP BY partner_id, uuid_click) AS cb
               RIGHT JOIN (SELECT dt,
                                  said,
                                  event_uuid,
                                  concat(domainWithoutWWW(source_url), path(source_url)) AS page_url,
                                  replaceRegexpAll(keyword,
                                                   concat('^[', regexpQuoteMeta(' '), ']*|[', regexpQuoteMeta(' '),
                                                          ']*$'), '')                    AS keyword,
                                  ad_unit_version,
                                  1                                                      AS clicks
                           FROM addotnet.raw_user_events
                           WHERE (sid = '11172302')
                             AND (event_name = 'presearchClick')
                             AND (dt >= '2021-01-01')) AS ruel_ce ON ruel_ce.event_uuid = cb.partner_id) AS click_stats
UNION ALL
SELECT dt,
       said,
       page_url,
       COALESCE(ad_unit_version, '')    AS ad_unit_version,
       COALESCE(ad_unit_type, '')       AS ad_unit_type,
       COALESCE(keyword, '')            AS keyword,
       COALESCE(keyword_impressions, 0) AS keyword_impressions,
       0                                AS clicks,
       0                                AS raw_clicks,
       0                                AS paid_clicks,
       ''                               AS uuid_click,
       COALESCE(dnl, '')                AS dnl,
       COALESCE(query_keyword, '')      AS query_keyword,
       COALESCE(page_clicks, 0)         AS page_clicks,
       COALESCE(num_ads, 0)             AS num_ads
FROM (SELECT rue_imp.dt                AS dt,
             rue_imp.said              AS said,
             rue_imp.page_url          AS page_url,
             replaceRegexpAll(ads_version.text, concat('^[', regexpQuoteMeta(' '), ']*|[', regexpQuoteMeta(' '), ']*$'),
                              '')      AS keyword,
             ads_version.query_keyword AS query_keyword,
             ads_version.dnl           AS dnl,
             rue_imp.ad_unit_version   AS ad_unit_version,
             ads_version.type          AS ad_unit_type,
             rue_imp.impressions       AS keyword_impressions,
             rue_imp.clicks            AS page_clicks,
             ads_counts.num_ads        AS num_ads
      FROM (SELECT dt,
                   said,
                   concat(domainWithoutWWW(source_url), path(source_url)) AS page_url,
                   ad_unit_version,
                   SUM(multiIf(event_name = 'presearchShown', 1, 0))      AS impressions,
                   SUM(multiIf(event_name = 'presearchClick', 1, 0))      AS clicks
            FROM addotnet.raw_user_events
            WHERE (sid = '11172302')
              AND (event_name IN ('presearchShown', 'presearchClick'))
              AND (dt >= '2021-01-01')
            GROUP BY dt, said, page_url, ad_unit_version) AS rue_imp
               LEFT JOIN (SELECT concat(domainWithoutWWW(page_url), path(page_url))                 AS page_url,
                                 version,
                                 type,
                                 text,
                                 multiIf(link LIKE '%chewy%', 'chewy', link LIKE '%petsmart%', 'petsmart',
                                         link LIKE '%dogtime.com%', 'airfind', link LIKE '%search-checker%', 'adsbrain',
                                         link LIKE '%shopmasse%', 'funnel', domainWithoutWWW(link)) AS dnl,
                                 replaceRegexpAll(extractURLParameter(link, 'q'), '(%20|\\+)', ' ') AS query_keyword
                          FROM (SELECT DISTINCT page_url, version, text, link, type
                                FROM addotnet.adunit_version) AS keyword_version) AS ads_version
                         ON (rue_imp.page_url = ads_version.page_url) AND
                            (rue_imp.ad_unit_version = ads_version.version)
               LEFT JOIN (SELECT concat(domainWithoutWWW(page_url), path(page_url)) AS page_url,
                                 version,
                                 type,
                                 COUNT(*)                                           AS num_ads
                          FROM addotnet.adunit_version
                          GROUP BY page_url, type, version) AS ads_counts
                         ON (rue_imp.page_url = ads_counts.page_url) AND
                            (rue_imp.ad_unit_version = ads_counts.version) AND
                            (ads_version.type = ads_counts.type)) AS impression;


CREATE VIEW addotnet.presearch_performance_view
AS
SELECT dt,
       said,
       pp.page_url                                                   AS page_url,
       COALESCE(tags, [])                                            AS tags,
       ad_unit_version,
       MAX(ad_unit_type)                                             AS ad_unit_type,
       keyword,
       multiIf((SUM(keyword_impressions) = 0) OR (SUM(num_ads) = 0), 0, SUM(page_clicks) > 0,
               CAST((SUM(keyword_impressions) * SUM(clicks)) / SUM(page_clicks), 'INT'),
               CAST(SUM(keyword_impressions) / SUM(num_ads), 'INT')) AS keyword_impressions,
       SUM(clicks)                                                   AS keyword_clicks,
       SUM(raw_clicks)                                               AS raw_clicks,
       SUM(paid_clicks)                                              AS paid_clicks,
       COALESCE(SUM(actions), 0)                                     AS actions,
       COALESCE(SUM(cpa_goal * actions_worth), 0)                    AS roi,
       MAX(dnl)                                                      AS dnl,
       MAX(query_keyword)                                            AS query_keyword
FROM (SELECT uuid_click, actions_worth, 1 AS actions, cpa_goal
      FROM addotnet.ad_event_view
      WHERE (sid = 11172302)
        AND (event_type = 'action')
        AND (dt >= '2021-01-01')
        AND (adgroup_lid != -7397529490732676256)
        AND (adgroup_lid != -8303520328960578099)) AS ab
         RIGHT JOIN (SELECT * FROM addotnet.presearch_performance_etl WHERE dt >= '2021-01-01') AS pp
                    ON pp.uuid_click = ab.uuid_click
         LEFT JOIN (SELECT page_url, groupArray((owner, tag)) AS tags
                    FROM addotnet.presearch_page_url_tagging
                    GROUP BY page_url) AS tagging ON pp.page_url = tagging.page_url
GROUP BY dt, said, pp.page_url, tags, ad_unit_version, keyword