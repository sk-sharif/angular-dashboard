CREATE TABLE
    addotnet.propel_domain_id_mapping
(
    domain_id String,
    domain String,
    dt DATE

) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/propel_domain_id_mapping/{shard}', '{replica}')
      PARTITION BY (dt)
      ORDER BY dt
      SETTINGS index_granularity = 8192;

