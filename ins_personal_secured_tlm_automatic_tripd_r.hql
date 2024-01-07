set hive.execution.engine=tez;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.compress.intermediate=true;
set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set hive.intermediate.compression.type=BLOCK;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.optimize.skewjoin=true;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.use.nonstaged=false;
set hive.vectorized.execution.reduce.groupby.enabled=true;
set mapred.reduce.tasks=40;
set mapred.max.split.size=67108864;
set hive.skewjoin.key=100000;
set hive.skewjoin.mapjoin.map.tasks=10000;
set hive.skewjoin.mapjoin.min.split=33554432;
set hive.auto.convert.join.noconditionaltask.size=10000000;
set mapred.map.child.java.opts="-Xmx5G";
set mapred.reduce.child.java.opts="-Xmx5G";

use personal_secured;

INSERT INTO TABLE  personal_secured.tlm_automatic_tripd_r PARTITION (date_partition)
SELECT
null as brand_cd, 
null as program_nme, 
null as product_cd, 
trip_id,
vchr_nbr,
point_timestamp,	
point_local_dtm,	
point_velocity,	
rpm,
to_date(to_utc_timestamp(from_unixtime(floor(point_timestamp/1000)), 'EST')) as date_partition
FROM stg_personal_secured.tlm_automatic_tripd_r_stg;


