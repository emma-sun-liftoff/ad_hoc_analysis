
-- total opportunity 
SELECT 
app_id
, count(DISTINCT IF(is_uncredited, device_id_sha1, null)) AS unattributed_users
, count(DISTINCT IF(is_uncredited, null, device_id_sha1)) AS attributed_users
FROM rtb.raw_installs
WHERE  date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24
	AND app_id = 1579
GROUP BY 1



-- user stats 
WITH temp AS (SELECT 
device_id_sha1 AS device_id
, is_uncredited 
, CASE WHEN try(IF(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) >= 0,
  CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64), 
  (CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64))+1)) < 0.000125 THEN 'yes' ELSE 'no' END AS in_no_bid_user_sample
, CASE WHEN try(IF(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) >= 0,
  CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64), 
  (CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64))+1)) < 0.0002   THEN 'yes' ELSE 'no' END AS in_bid_user_sample
FROM rtb.raw_installs 
WHERE  date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24
	AND app_id = 1579)


SELECT 
is_uncredited
, in_bid_user_sample
--, in_no_bid_user_sample
count(DISTINCT device_id) AS user_count
FROM temp
GROUP BY 1,2



-- User Level Funnel

WITH funnel AS (
SELECT 
count(*) AS total_bids 
, 0 AS total_no_bids
, 0 AS impressions 
, 0 AS installs 
FROM rtb.user_sampled_bids 
WHERE bid_request__device__platform_specific_id_sha1 = '5e7b2e57c77b01007d91d7f2d34dda2aab8ce24c'
AND date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24

UNION ALL 
SELECT 
0 AS total_bids 
, count(*) AS total_no_bids
, 0 AS impressions 
, 0 AS installs 
FROM rtb.user_sampled_no_bids 
WHERE bid_request__device__platform_specific_id_sha1 = '5e7b2e57c77b01007d91d7f2d34dda2aab8ce24c'
AND date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24

UNION ALL 
SELECT
0 AS total_bids 
, 0 AS total_no_bids 
, count(*) AS impressions
, 0 AS installs 
FROM rtb.impressions_with_bids 
WHERE bid__bid_request__device__platform_specific_id_sha1 = '5e7b2e57c77b01007d91d7f2d34dda2aab8ce24c'
AND date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24

UNION ALL 
SELECT 
0 AS total_bids 
, 0 AS total_no_bids 
, 0 AS impressions 
, count(*) AS installs 
FROM rtb.installs 
WHERE ad_click__impression__bid__bid_request__device__platform_specific_id_sha1 = '5e7b2e57c77b01007d91d7f2d34dda2aab8ce24c'
AND date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24
)


SELECT
sum(total_bids) AS total_bids
, sum(total_no_bids) AS total_no_bids
, sum(impressions) AS impressions
, sum(installs) AS installs
FROM funnel



-- unattributed funnel
WITH temp AS (SELECT 
device_id_sha1 AS device_id
, is_uncredited 
, CASE WHEN try(IF(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) >= 0,
  CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64), 
  (CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64))+1)) < 0.000125 THEN 'yes' ELSE 'no' END AS in_no_bid_user_sample
, CASE WHEN try(IF(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) >= 0,
  CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64), 
  (CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64))+1)) < 0.0002   THEN 'yes' ELSE 'no' END AS in_bid_user_sample
FROM rtb.raw_installs 
WHERE  date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24
	AND app_id = 1579)


, devices AS (SELECT 
DISTINCT device_id
FROM temp
WHERE in_bid_user_sample = 'yes' AND in_no_bid_user_sample = 'yes'
AND is_uncredited = TRUE
)



, funnel AS (
SELECT 
count(*) AS total_bids 
, 0 AS total_no_bids
, 0 AS impressions 
, 0 AS installs 
FROM rtb.user_sampled_bids 
WHERE bid_request__device__platform_specific_id_sha1 IN (SELECT device_id FROM devices)
AND date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24

UNION ALL 
SELECT 
0 AS total_bids 
, count(*) AS total_no_bids
, 0 AS impressions 
, 0 AS installs 
FROM rtb.user_sampled_no_bids 
WHERE bid_request__device__platform_specific_id_sha1 IN (SELECT device_id FROM devices)
AND date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24

UNION ALL 
SELECT
0 AS total_bids 
, 0 AS total_no_bids 
, count(*) AS impressions
, 0 AS installs 
FROM rtb.impressions_with_bids 
WHERE bid__bid_request__device__platform_specific_id_sha1 IN (SELECT device_id FROM devices)
AND date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24

UNION ALL 
SELECT 
0 AS total_bids 
, 0 AS total_no_bids 
, 0 AS impressions 
, count(*) AS installs 
FROM rtb.installs 
WHERE ad_click__impression__bid__bid_request__device__platform_specific_id_sha1 IN (SELECT device_id FROM devices)
AND date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24
)



SELECT
sum(total_bids) AS total_bids
, sum(total_no_bids) AS total_no_bids
, sum(impressions) AS impressions
, sum(installs) AS installs
FROM funnel



-- [To validate if see bid requests] 

SELECT 
count(DISTINCT ri.device_id_sha1)
, is_uncredited 
FROM rtb.raw_installs ri
INNER JOIN (SELECT 
DISTINCT bid_request__device__platform_specific_id_sha1
FROM rtb.no_bids
WHERE date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 30*24) AS tt 
	ON tt.bid_request__device__platform_specific_id_sha1 = ri.device_id_sha1
WHERE  date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24
	AND ri.app_id = 1579
GROUP BY 2
		
	
SELECT 
count(DISTINCT ri.device_id_sha1)
, is_uncredited 
FROM rtb.raw_installs ri
INNER JOIN (SELECT 
DISTINCT bid_request__device__platform_specific_id_sha1
FROM rtb.bids
WHERE date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 30*24
	AND bid_request__device__platform_specific_id_sha1 = '8022df72298f04009d34c15f13a0c4d94f1b8e72') AS tt 
	ON tt.bid_request__device__platform_specific_id_sha1 = ri.device_id_sha1
WHERE  date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 24
	AND ri.app_id = 1579
GROUP BY 2

