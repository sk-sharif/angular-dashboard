select hour_id,count(*) from live_etl.search_request where url not like '%skip_incoming_logging=true%' AND dt = "2021-04-15"  AND uuid is Not null and uuid <> '' and uuid <> '-'
group by hour_id order by hour_id;

SELECT hour_id,
       count(*)
FROM live_etl.search_entry_point_click
WHERE dt = "2021-04-15"
GROUP BY hour_id
ORDER BY hour_id;


SELECT mc.hour_id,SUM(CAST(CASE
                               WHEN epc.reason_for_unpaid IS NOT NULL
                                   AND epc.reason_for_unpaid != 'null' THEN NULL
                               WHEN epc.reason_for_unpaid != 'null' THEN NULL
                               WHEN epc.masked_click
                                   AND mc.uuid IS NULL THEN NULL
                               WHEN mc.reason_for_unpaid IS NOT NULL
                                   AND mc.reason_for_unpaid!= 'null' THEN NULL
                               WHEN mc.uuid IS NOT NULL
                                   AND epc.uuid_click IS NOT NULL THEN 1
                               ELSE 0
    END AS INT)) paid_clicks,
       SUM(CASE
               WHEN epc.reason_for_unpaid IS NOT NULL
                   AND epc.reason_for_unpaid != 'null' THEN NULL
               WHEN epc.reason_for_unpaid != 'null' THEN NULL
               WHEN epc.masked_click
                   AND mc.uuid IS NULL THEN NULL
               WHEN mc.reason_for_unpaid IS NOT NULL
                   AND mc.reason_for_unpaid!= 'null' THEN NULL
               WHEN mc.uuid IS NOT NULL
                   AND epc.uuid_click IS NOT NULL THEN mc.gross_bid
               ELSE 0
           END) revenue
FROM live_etl.search_masked_click mc
         JOIN live_etl.search_entry_point_click epc ON mc.uuid = epc.uuid_click
WHERE (mc.reason_for_unpaid IS NULL
    OR mc.reason_for_unpaid ='null')
  AND epc.dt IN ('2021-04-15')
  AND mc.dt IN ('2021-04-15')
GROUP BY mc.hour_id
ORDER BY mc.hour_id;


select hour_id,sum(eventpixel_actions_worth) from live_etl.search_action WHERE dt = "2021-04-15"
GROUP BY hour_id
ORDER BY hour_id;

