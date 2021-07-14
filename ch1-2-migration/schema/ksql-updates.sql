--Click stream updates: handle newly added columns
DROP STREAM IF EXISTS COM_FINDOLOGY_MODEL_TRAFFIC_LOG_ENTRYPOINTCLICKLOGEVENT_AVRO_CH1_2;

CREATE STREAM com_findology_model_traffic_log_EntryPointClickLogEvent_avro_ch1_2 WITH (KAFKA_TOPIC='com_findology_model_traffic_log_EntryPointClickLogEvent', VALUE_FORMAT='AVRO', KEY='uuid_click');

DROP STREAM IF EXISTS COM_FINDOLOGY_MODEL_TRAFFIC_LOG_FRAUDCHECKASYNCCALLBACKLOGEVENT_AVRO_CH1_2;

CREATE STREAM com_findology_model_traffic_log_FraudCheckAsyncCallbackLogEvent_avro_ch1_2 WITH (KAFKA_TOPIC='com_findology_model_traffic_log_FraudCheckAsyncCallbackLogEvent', VALUE_FORMAT='AVRO', KEY='uuid');

CREATE STREAM  CLICK_CH1_2 WITH (PARTITIONS=10, VALUE_FORMAT='JSON') AS
    SELECT
        EPC.*,
        MC.*,
        MC.UUID MC_UUID_CLICK,
        IFNULL(EPC.UUID, MC.SEARCHID) UUID
    FROM COM_FINDOLOGY_MODEL_TRAFFIC_LOG_FRAUDCHECKASYNCCALLBACKLOGEVENT_AVRO_CH1_2 MC FULL OUTER JOIN
        COM_FINDOLOGY_MODEL_TRAFFIC_LOG_ENTRYPOINTCLICKLOGEVENT_AVRO_CH1_2 EPC within 1 hour on EPC.UUID_CLICK =MC.UUID;
