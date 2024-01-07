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
insert into table personal_secured.telematics partition (point_dt) select 
vendor,
brand_cd,
program_nme,
product_cd,
type_id,
trip_id,
vchr_nbr,
point_time,
point_latitude,
point_longitude,
point_gps_qlty,
point_gps_prcsn,
point_lcl_time_offst,
event_cd,
event_intnsty,
event_durtn,
odmtr_readng,
avg_speed_during_event,
max_acclrtn_during_event,
yds_trvld,
time_elpsd,
rnd_time_elpsd,
point_speed,
point_speed_vss,
point_speed_gps,
point_hdng,
avg_speed_from_prev_point,
road_typ,
point_gmt_dtm,
point_gmt_int/1000,
point_local_dtm,
point_speed_diff,
point_source,
vehcl_rpm,
vndr_sys_trip_flg,
usr_trip_flg,
device_id,
trip_event_id,
trip_event_typ_cd,
src_date_partition,
point_dt
 from stg_personal_secured.automatic_telematics_stg;


