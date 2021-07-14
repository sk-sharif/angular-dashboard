ALTER TABLE addotnet.ad_event_opportunity_daily DROP PARTITION ('PROCESS_DATE');

INSERT INTO addotnet.ad_event_opportunity_daily
SELECT event_date,
       coalesce(ad_event.affiliate_account_lid, 0) affiliate_account_lid,
       coalesce(ad_event.affiliate_account_hid, 0) affiliate_account_hid,
       coalesce(ad_event.search_keyword, '')       search_keyword,
       sid,
       sum(requests)
from addotnet.ad_event
where event_type = 'request'
  and event_date = 'PROCESS_DATE'
group by event_date,
         affiliate_account_lid,
         affiliate_account_hid,
         search_keyword,
         sid;