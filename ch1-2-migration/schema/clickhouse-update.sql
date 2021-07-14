-- stop MVs
drop table etl.request_mat;
drop table etl.request_ad_return_mat;
drop table etl.request_feed_return_mat;
drop table etl.click_mat;
drop table etl.action_mat;

-- alter columns
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS channel Nullable(Int32) AFTER feed_returned;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS cookie Nullable(String) AFTER channel;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS data_size Nullable(Int64) AFTER cookie;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS device_id Nullable(String) AFTER data_size;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS domain_name Nullable(String) AFTER device_id;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_external_adlisting_id Nullable(String) AFTER domain_name;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_internal_ron_adgroup_key Nullable(String) AFTER highest_external_adlisting_id;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_internal_ron_adlisting_id Nullable(String) AFTER highest_internal_ron_adgroup_key;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_internal_ron_advertiser_key Nullable(String) AFTER highest_internal_ron_adlisting_id;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_internal_ron_bid Nullable(Float64) AFTER highest_internal_ron_advertiser_key;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_internal_ron_net_bid Nullable(Float64) AFTER highest_internal_ron_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_internal_targeted_adgroup_key Nullable(String) AFTER highest_internal_ron_net_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_internal_targeted_adlisting_id Nullable(String) AFTER highest_internal_targeted_adgroup_key;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_internal_targeted_advertiser_key Nullable(String) AFTER highest_internal_targeted_adlisting_id;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_internal_targeted_bid Nullable(Float64) AFTER highest_internal_targeted_advertiser_key;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS highest_internal_targeted_net_bid Nullable(Float64) AFTER highest_internal_targeted_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS http_type Nullable(String) AFTER highest_internal_targeted_net_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS internal_ron_adgroup_keys Nullable(String) AFTER http_type;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS internal_targeted_adgroup_keys Nullable(String) AFTER internal_ron_adgroup_keys;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS is_next_bidder_repeated_search Nullable(UInt8) AFTER internal_targeted_adgroup_keys;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS latency Nullable(Int64) AFTER is_next_bidder_repeated_search;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS max_external_bid Nullable(Float64) AFTER latency;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS max_external_net_bid Nullable(Float64) AFTER max_external_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS max_internal_bid Nullable(Float64) AFTER max_external_net_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS max_internal_net_bid Nullable(Float64) AFTER max_internal_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS next_bidder_repeated_search_num Nullable(Int64) AFTER max_internal_net_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS search_node_identifier Nullable(String) AFTER next_bidder_repeated_search_num;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS num_external_ads Nullable(Int32) AFTER search_node_identifier;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS num_internal_ads Nullable(Int32) AFTER num_external_ads;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS num_ron_ads Nullable(Int32) AFTER num_internal_ads;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS partner_id Nullable(String) AFTER num_ron_ads;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS predicted_searchiq_method Nullable(String) AFTER partner_id;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS predicted_searchiq_user_id Nullable(String) AFTER predicted_searchiq_method;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS protocol Nullable(String) AFTER predicted_searchiq_user_id;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS pub_user_id Nullable(String) AFTER protocol;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS publisher_referrer Nullable(String) AFTER pub_user_id;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS publisher_referrer_hostname Nullable(String) AFTER publisher_referrer;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS quality_bucket_key Nullable(String) AFTER publisher_referrer_hostname;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS raw_keyword Nullable(String) AFTER quality_bucket_key;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS reason_not_served Nullable(String) AFTER raw_keyword;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS remote_requester Nullable(String) AFTER reason_not_served;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS remote_system Nullable(String) AFTER remote_requester;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS remote_user Nullable(String) AFTER remote_system;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS server_request_port Nullable(Int32) AFTER remote_user;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS status_code Nullable(Int32) AFTER server_request_port;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS time_stamp Nullable(String) AFTER status_code;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS top_bid_feed_advertiser_id Nullable(Int64) AFTER time_stamp;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS top_external_bid_keyword Nullable(String) AFTER top_bid_feed_advertiser_id;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS top_internal_bid_keyword Nullable(String) AFTER top_external_bid_keyword;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS top_internal_bid_type Nullable(String) AFTER top_internal_bid_keyword;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS traffic_provider_key Nullable(String) AFTER top_internal_bid_type;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS url Nullable(String) AFTER traffic_provider_key;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS url_parameters_json Nullable(String) AFTER url;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS user_ip Nullable(String) AFTER url_parameters_json;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS x_forwarded_for Nullable(String) AFTER user_ip;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS able_to_set_cookie Nullable(UInt8) AFTER x_forwarded_for;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS anonymized_traffic Nullable(UInt8) AFTER able_to_set_cookie;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS category_key Nullable(String) AFTER anonymized_traffic;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS click_date Nullable(Int64) AFTER category_key;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS epc_created_on Nullable(Int64) AFTER click_date;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS mc_created_on Nullable(Int64) AFTER epc_created_on;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS feed_advertiser_advertiser_id Nullable(String) AFTER mc_created_on;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS feed_advertiser_click_url Nullable(String) AFTER feed_advertiser_advertiser_id;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS feed_advertiser_display_url Nullable(String) AFTER feed_advertiser_click_url;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS gross_bid Nullable(Float64) AFTER feed_advertiser_display_url;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS is_expanded_query Nullable(UInt8) AFTER gross_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS masked_click Nullable(UInt8) AFTER is_expanded_query;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS net_bid Nullable(Float64) AFTER masked_click;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS network_type Nullable(String) AFTER net_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS new_user Nullable(UInt8) AFTER network_type;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS next_hop_url Nullable(String) AFTER new_user;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS persistent_user_id_cookie Nullable(String) AFTER next_hop_url;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS rank Nullable(Int32) AFTER persistent_user_id_cookie;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS referrer_hostname Nullable(String) AFTER rank;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS referring_url Nullable(String) AFTER referrer_hostname;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS request_port Nullable(Int32) AFTER referring_url;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS said_category Nullable(String) AFTER request_port;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS said_tier Nullable(String) AFTER said_category;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS screen_height Nullable(Int32) AFTER said_tier;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS screen_width Nullable(Int32) AFTER screen_height;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS epc_node_identifier Nullable(String) AFTER screen_width;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS mc_node_identifier Nullable(String) AFTER epc_node_identifier;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS search_referrer_hostname Nullable(String) AFTER mc_node_identifier;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS searchid Nullable(String) AFTER search_referrer_hostname;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS searchiquserid Nullable(String) AFTER searchid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS sp_adgroup Nullable(String) AFTER searchiquserid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS sp_category Nullable(String) AFTER sp_adgroup;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS sp_feed_bid Nullable(Float64) AFTER sp_category;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS sp_target Nullable(String) AFTER sp_feed_bid;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS viewable_status Nullable(String) AFTER sp_target;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS viewed_url Nullable(String) AFTER viewable_status;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS window_height Nullable(Int32) AFTER viewed_url;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS window_position_left Nullable(Int32) AFTER window_height;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS window_position_top Nullable(Int32) AFTER window_position_left;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS window_width Nullable(Int32) AFTER window_position_top;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS action_date Nullable(String) AFTER window_width;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS action_epoch Nullable(Int64) AFTER action_date;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS additional_get_parameters Nullable(String) AFTER action_epoch;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS batch Nullable(Int8) AFTER additional_get_parameters;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS benchmark_keyword Nullable(String) AFTER batch;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS listing_id Nullable(String) AFTER benchmark_keyword;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS matched_keyword Nullable(String) AFTER listing_id;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS action_node_identifier Nullable(String) AFTER matched_keyword;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS received_epoch Nullable(Int64) AFTER action_node_identifier;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS sp_query_keyword Nullable(String) AFTER received_epoch;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS t_get_parameter Nullable(String) AFTER sp_query_keyword;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS target_expanded_query_keyword Nullable(String) AFTER t_get_parameter;
ALTER TABLE addotnet.ad_event ON CLUSTER Replicated
    ADD COLUMN IF NOT EXISTS target_type Nullable(String) AFTER target_expanded_query_keyword;


-- recreate MVs with updated columns handling from clickhouse.sql