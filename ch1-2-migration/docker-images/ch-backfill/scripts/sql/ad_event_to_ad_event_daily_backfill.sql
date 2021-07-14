create view if not exists addotnet.backfill_ad_event_daily_view
AS
(
    select dt,
           event_date,
           event_year,
           advertiser_lid,
           advertiser_hid,
           campaign_id,
           adgroup_lid,
           adgroup_hid,
           provider_account_lid as provider_account_lid,
           provider_account_hid as provider_account_hid,
           feed_advertiser_id   as feed_advertiser_id_1,
           affiliate_account_lid,
           affiliate_account_hid,
           sid,
           said,
           requests,
           ad_returns,
           raw_clicks,
           paid_clicks,
           pub_payout,
           revenue,
           dollars_worth,
           actions_worth
    from addotnet.ad_event
    where (event_type in ('request', 'ad_return', 'click'))
       or (event_type = 'action' and feed_advertiser_id <> 72)
       or (event_type = 'action' and feed_advertiser_id is null))
UNION ALL
(select dt,
        event_date,
        event_year,
        advertiser_lid,
        advertiser_hid,
        campaign_id,
        adgroup_lid,
        adgroup_hid,
        0 as provider_account_lid,
        0 as provider_account_hid,
        0 as feed_advertiser_id_1,
        affiliate_account_lid,
        affiliate_account_hid,
        sid,
        said,
        requests,
        ad_returns,
        raw_clicks,
        paid_clicks,
        pub_payout,
        revenue,
        dollars_worth,
        actions_worth
 from addotnet.ad_event
 where (event_type = 'action' and feed_advertiser_id = 72)
);



ALTER TABLE addotnet.ad_event_daily on CLUSTER Replicated
    DROP PARTITION ('PROCESS_DATE');

INSERT INTO addotnet.ad_event_daily
SELECT event_date,
       toYYYYMM(event_date)                                                   event_month_id,
       toString(formatDateTime(event_date, '%Y-%m'))                          event_month_name,
       event_year,
       coalesce(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END) advertiser_lid,
       coalesce(CASE WHEN advertiser_hid = -1 THEN 0 ELSE advertiser_hid END) advertiser_hid,
       coalesce(ad_event.campaign_id, 0)                                      campaign_id,
       coalesce(ad_event.adgroup_lid, 0)                                      adgroup_lid,
       coalesce(ad_event.adgroup_hid, 0)                                      adgroup_hid,
       coalesce(ad_event.provider_account_lid, 0)                             provider_account_lid,
       coalesce(ad_event.provider_account_hid, 0)                             provider_account_hid,
       coalesce(ad_event.feed_advertiser_id_1, 0)                             feed_advertiser_id,
       coalesce(ad_event.affiliate_account_lid, 0)                            affiliate_account_lid,
       coalesce(ad_event.affiliate_account_hid, 0)                            affiliate_account_hid,
       sid,
       said,
       SUM(requests),
       SUM(ad_returns),
       SUM(raw_clicks),
       SUM(paid_clicks),
       SUM(pub_payout),
       SUM(revenue),
       SUM(dollars_worth),
       SUM(actions_worth),
       SUM(case
               when actions_worth > 0 then 1
               when actions_worth < 0 then -1
               else 0 end) as                                                 event_fires_count
FROM addotnet.backfill_ad_event_daily_view as ad_event
WHERE dt >= 'PREV_DATE'
  and event_date between 'PROCESS_DATE' and 'PROCESS_DATE'
GROUP BY event_date,
         event_month_id,
         event_month_name,
         event_year,
         advertiser_lid,
         advertiser_hid,
         campaign_id,
         adgroup_lid,
         adgroup_hid,
         provider_account_lid,
         provider_account_hid,
         feed_advertiser_id,
         affiliate_account_lid,
         affiliate_account_hid,
         sid,
         said;


INSERT INTO addotnet.ad_event_daily
SELECT dt                                                                     event_date,
       toYYYYMM(event_date)                                                   event_month_id,
       toString(formatDateTime(event_date, '%Y-%m'))                          event_month_name,
       toUInt16(formatDateTime(event_date, '%Y'))                             event_year,
       coalesce(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END) advertiser_lid,
       coalesce(CASE WHEN advertiser_hid = -1 THEN 0 ELSE advertiser_hid END) advertiser_hid,
       coalesce(campaign_id, 0)                                               campaign_id,
       coalesce(adgroup_lid, 0)                                               adgroup_lid,
       coalesce(adgroup_hid, 0)                                               adgroup_hid,
       coalesce(provideraccount_lid, 0)                                       provider_account_lid,
       coalesce(provideraccount_hid, 0)                                       provider_account_hid,
       coalesce(feed_id, 0)                                                   feed_advertiser_id,
       coalesce(affiliateaccount_lid, 0)                                      affiliate_account_lid,
       coalesce(affiliateaccount_hid, 0)                                      affiliate_account_hid,
       sid,
       ''                                                                     said,
       0                                                                      requests,
       0                                                                      ad_returns,
       0                                                                      raw_clicks,
       0                                                                      paid_clicks,
       0                                                                      pub_payout,
       0                                                                      revenue,
       0                                                                      dollars_worth,
       0                                                                      actions_worth,
       0                                                                      event_fires_count
FROM addotnet.media_buyer_adjustment
WHERE (dt = 'PROCESS_DATE')
  AND ((mb_cost > 0) OR (mb_clicks > 0))
  AND ((dt, affiliateaccount_lid, sid, provideraccount_lid, feed_id, advertiser_lid, campaign_id, adgroup_lid) NOT IN
       (
           SELECT event_date,
                  affiliate_account_lid,
                  sid,
                  provider_account_lid,
                  feed_advertiser_id,
                  advertiser_lid,
                  campaign_id,
                  adgroup_lid
           FROM ad_event_daily
           WHERE dictGet('addotnet.affiliate_account_dim', 'is_media_buy', toUInt64(ABS(affiliate_account_lid))) = 1
             and (event_date = 'PROCESS_DATE')
       ));