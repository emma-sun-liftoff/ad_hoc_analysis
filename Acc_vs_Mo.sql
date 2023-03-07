-- All Auction Stats
with dedupe as (
  select *, (lo_bid_price - mo_bid_price) as lo_minus_mo_bid from liftoff_moloco_jaeger_logs_agg
  --where hr = 1
)
select 
  req_os
  , req_payout_type
  , count(*) as total_lo_mo_auctions
  , sum(lo_bid_price) as lo_bid_price
  , sum(mo_bid_price) as mo_bid_price
  , sum(case when lo_bid_price is not null then 1 else 0 end) as lo_bids_count
  , sum(case when mo_bid_price is not null then 1 else 0 end) as mo_bids_count
  , sum(case when lo_minus_mo_bid is not null then 1 else 0 end) as both_bids_count
--  , sum(case when lo_minus_mo_bid >= 0 then 1 else 0 end) as lo_bid_higher
--  , sum(case when lo_minus_mo_bid < 0 then 1 else 0 end) as lo_bid_lower
  , sum(case when lo_is_winner and mo_bid_price is not null then lo_bid_price end) as lo_bids_price_when_wins_when_both_bid
  , sum(case when mo_is_winner and lo_bid_price is not null then mo_bid_price end) as mo_bids_price_when_wins_when_both_bid
  , sum(case when lo_is_winner and mo_bid_price is not null then 1 else 0 end) as lo_wins_when_both_bid
  , sum(case when mo_is_winner and lo_bid_price is not null then 1 else 0 end) as mo_wins_when_both_bid
    
--  , sum(case when lo_minus_mo_bid >= 0 then (lo_bid_price - mo_bid_price / lo_bid_price) else null end) / sum(case when lo_minus_mo_bid >= 0 then 1 else 0 end) as lo_bid_higher_by_percent_average
--  , sum(case when lo_minus_mo_bid < 0 then (lo_bid_price - mo_bid_price / mo_bid_price) else null end) / sum(case when lo_minus_mo_bid < 0 then 1 else 0 end) as lo_bid_lower_by_percent_average
--  , sum(case when lo_minus_mo_bid >= 0 then lo_minus_mo_bid else null end) / sum(case when lo_minus_mo_bid >= 0 then 1 else 0 end) as lo_bid_higher_difference_average
--  , sum(case when lo_minus_mo_bid < 0 then lo_minus_mo_bid else null end) / sum(case when lo_minus_mo_bid < 0 then 1 else 0 end) as lo_bid_lower_difference_average
--  , avg(case when lo_minus_mo_bid is not null then lo_minus_mo_bid else null end) as avg_bid_diff
from dedupe
where mo_resp_adomain in (array('playrix.com')) OR lo_resp_adomain in (array('playrix.com'))
 --, array('king.com'))
group by 1,2

-- FlatCPM Bid Premiums

with auctions as (
  select *
  from edsp_transactions
  where timestamp_at_delivery >= getdate() - interval '10 days' 
  --and geoip_country_code_at_delivery  = 'US' 
  and dev_platform IN ('android', 'iOS')
  and publisher_payout_type = 'FLAT_CPM'
  --and is_header_bidding 
  --and supply_name_at_hbp_notification in ('max', 'ironsource')
  and placement_type in ('video','banner')
  --and second_place_price is not null
  --and winning_bid_price >= bid_floor
  and start_at_tpat is not null
  and winner_id in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792',
     '60f0c4687a479f0016f763e2', '60f0c55d8b63dd00161567a6', '5bc0e200eb95ad0016cfb668', '5f86383c18eece0010058e72')
  and second_place_rtb_id in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792',
  '60f0c4687a479f0016f763e2', '60f0c55d8b63dd00161567a6', '5bc0e200eb95ad0016cfb668', '5f86383c18eece0010058e72',
  '60adc79dfb70f80016e36884', '5a3320e19fcb7d590c0034ba', '59128f4fa8e4b2cbcb250c87', '5a376061b1fd0e0016b88e7b', '5d4ce4726f4447f538dfd7e9')
  and third_place_rtb_id in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792',
    '60f0c4687a479f0016f763e2', '60f0c55d8b63dd00161567a6', '5bc0e200eb95ad0016cfb668', '5f86383c18eece0010058e72',
    '60adc79dfb70f80016e36884', '5a3320e19fcb7d590c0034ba', '59128f4fa8e4b2cbcb250c87', '5a376061b1fd0e0016b88e7b', '5d4ce4726f4447f538dfd7e9')
)

select dev_platform
  , placement_type
  , case when geoip_country_code_at_delivery = 'US' then 'US' else 'non-US' end as "geo"
  , 'vx' as supply_name
  , case when winner_id in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792') then 'Liftoff'
    else 'Moloco' end as dsp
  , case when second_place_price is null then false else true end as has_second_bidder
  
  , sum(winning_bid_price) as winning_bids_sum_gross
  , sum(coalesce(second_place_price, bid_floor )) as market_price_sum
  , (winning_bids_sum_gross / market_price_sum - 1) as premium_gross
  
from auctions

group by 1,2,3,4,5,6
order by 1,2,3,4,5,6



---------------------
-- IAB Bid Premiums

with auctions as (
  select hb.*
    , case when edsp.second_place_rtb_id in ('60adc79dfb70f80016e36884', '5a3320e19fcb7d590c0034ba', '59128f4fa8e4b2cbcb250c87', '5a376061b1fd0e0016b88e7b', '5d4ce4726f4447f538dfd7e9') then edsp.second_place_price
      else edsp.second_place_price end as second_price_internal_auction
    , bidrequest_device_os_at_auction as platform
    , bidrequest_imp_type_at_auction as placement_type
    , case when bidrequest_geo_country_at_auction = 'USA' then 'US' else 'non-US' end as geo
    -- , supply_name
    , case 
      when bidder_id_at_auction in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792') then 'Liftoff'
      when bidder_id_at_auction in ('60adc79dfb70f80016e36884', '5a3320e19fcb7d590c0034ba', '59128f4fa8e4b2cbcb250c87', '5a376061b1fd0e0016b88e7b', '5d4ce4726f4447f538dfd7e9') then 'Vungle'
      else 'Moloco'
      end as dsp
    , case when second_highest_bid_price_at_notification is null then false else true end as has_second_bidder
  from public.hbp_transactions_with_bids hb
  left join public.edsp_transactions as edsp on edsp.event_id = hb.eventid_or_bidid
  where timestamp_at_auction >= getdate() - interval '10 days' 
    and (is_bill_at_notification) 
    and bidder_id_at_auction in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792',
      '60f0c4687a479f0016f763e2', '60f0c55d8b63dd00161567a6', '5bc0e200eb95ad0016cfb668', '5f86383c18eece0010058e72',
      '60adc79dfb70f80016e36884', '5a3320e19fcb7d590c0034ba', '59128f4fa8e4b2cbcb250c87', '5a376061b1fd0e0016b88e7b', '5d4ce4726f4447f538dfd7e9')
    and hb.supply_name in ('max','ironsource')
    and bidrequest_device_os_at_auction in ('android', 'iOS')
    --and bidrequest_imp_type_at_auction in ('video')
    and (second_highest_bid_price_at_notification <= bid_price_at_notification or second_highest_bid_price_at_notification is null)
    and second_place_rtb_id in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792',
      '60f0c4687a479f0016f763e2', '60f0c55d8b63dd00161567a6', '5bc0e200eb95ad0016cfb668', '5f86383c18eece0010058e72',
      '60adc79dfb70f80016e36884', '5a3320e19fcb7d590c0034ba', '59128f4fa8e4b2cbcb250c87', '5a376061b1fd0e0016b88e7b', '5d4ce4726f4447f538dfd7e9')
    and third_place_rtb_id in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792',
      '60f0c4687a479f0016f763e2', '60f0c55d8b63dd00161567a6', '5bc0e200eb95ad0016cfb668', '5f86383c18eece0010058e72',
      '60adc79dfb70f80016e36884', '5a3320e19fcb7d590c0034ba', '59128f4fa8e4b2cbcb250c87', '5a376061b1fd0e0016b88e7b', '5d4ce4726f4447f538dfd7e9')
),

bid_sums as (
select 
  platform
  , placement_type
  , geo
  , supply_name
  , dsp
  , has_second_bidder
  
  , sum(adx_bid_price_at_auction) as winning_bids_sum_gross
  , sum(bid_price_at_notification) as winning_bids_sum_net
  
  , 1 - winning_bids_sum_net / winning_bids_sum_gross  as vx_margin
from auctions

group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
)

select 
  a.platform
  , a.placement_type
  , a.geo
  , a.supply_name
  , a.dsp
  , a.has_second_bidder
  
  , winning_bids_sum_gross
  , winning_bids_sum_net
  , vx_margin
  
  -- Second price in the internal auction pulled from the edsp_transactions includes Vungle margin
  -- Second price and floor in the external auction pulled from hbp_transactions does not include Vungle margin - those are raw numbers reported by mediation
  , sum(greatest(second_highest_bid_price_at_notification , second_price_internal_auction * (1 - vx_margin), bidrequest_imp_bidfloor_at_auction , 0)) / (1 - vx_margin) as market_price_sum_gross
  , market_price_sum_gross * (1 - vx_margin) as market_price_sum_net
  
  , (winning_bids_sum_gross / market_price_sum_gross - 1) as premium_gross
  , (winning_bids_sum_net / market_price_sum_net - 1) as premium_net

from auctions a
left join bid_sums USING (platform
  , placement_type
  , geo
  , supply_name
  , dsp
  , has_second_bidder)

group by 1,2,3,4,5,6,7,8,9
order by 1,2,3,4,5,6,7,8,9






-- targeted devices
with dev_counts as (
select winning_bid_bundle, trunc(date_trunc('week', timestamp_at_impression)) as imp_week, dev_id_at_impression
  , sum(case when winner_id in ( '60f0c4687a479f0016f763e2', '60f0c55d8b63dd00161567a6', '5bc0e200eb95ad0016cfb668', '5f86383c18eece0010058e72') then 1 else null end) as mo_impressions
  , sum(case when winner_id in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792') then 1 else null end) as lo_impressions
from edsp_transactions
where winner_id in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792', '60f0c4687a479f0016f763e2', '60f0c55d8b63dd00161567a6', '5bc0e200eb95ad0016cfb668', '5f86383c18eece0010058e72')
  and timestamp_at_impression >= getdate() - interval '28 days'
  and dev_platform = 'android'
  --and geoip_country_code_at_impression = 'US'
  --and adomain in ('king.com', 'playrix.com')
  and (winning_bid_bundle ILIKE '%.playrix.%')
  --or winning_bid_bundle ILIKE '%.playrix.%')
group by 1,2, 3
--order by 2 desc
),

medians as (
  select winning_bid_bundle, imp_week
    , median(1.00 * lo_impressions) as lo_imp_per_device_median
  from dev_counts
  group by 1,2
)

select winning_bid_bundle, imp_week
  , medians.lo_imp_per_device_median
  , count(dev_id_at_impression) as dev_count
  , sum(case when mo_impressions is not null and lo_impressions is not null then 1 else 0 end) as dev_count_when_both_targeted
  , sum(case when mo_impressions is null and lo_impressions is not null then 1 else 0 end) as dev_count_when_LO_targeted
  , sum(case when mo_impressions is not null and lo_impressions is null then 1 else 0 end) as dev_count_when_MO_targeted
  , avg(1.00 * mo_impressions) as mo_imp_per_device
  , avg(1.00 * lo_impressions) as lo_imp_per_device
  , median(1.00 * mo_impressions) as mo_imp_per_device_median
  , 1.00 * sum(case when mo_impressions is not null and lo_impressions is not null then mo_impressions else 0 end) / nullif(dev_count_when_both_targeted, 0) as mo_imp_per_device_when_both_targeted
  , 1.00 * sum(case when mo_impressions is not null and lo_impressions is not null then lo_impressions else 0 end) / nullif(dev_count_when_both_targeted, 0) as lo_imp_per_device_when_both_targeted
from dev_counts
left join medians using(winning_bid_bundle, imp_week)
group by 1,2,3
order by 1,2

-- device level win rate

select winning_bid_bundle, 
 count(dev_id_at_impression) as total_user
  , sum(case when winner_id in ( '60f0c4687a479f0016f763e2', '60f0c55d8b63dd00161567a6', '5bc0e200eb95ad0016cfb668', '5f86383c18eece0010058e72') then 1 else null end) as mo_impressions
  , sum(case when winner_id in ('6279c195e1e538a5d1e64090', '627afba9cc8bd7121ff60426', '627afcad6763d32d545b71d7', '6189c34a478cdcadb14300f8', '61c276a2c0b8ef544ed8d9c1', '61c27759c0b8ef544ed8d9c3', '61c277c0d3819d69caeeba19', '5f6415539290720015d69d84', '5f6415d4fd2311001528e9ef', '5f74d16d466598000fe30644', '5f7b8e8efeb062001649e792') then 1 else null end) as lo_impressions
from edsp_transactions
where
   timestamp_at_impression >= getdate() - interval '28 days'
  and dev_platform = 'android'
  --and geoip_country_code_at_impression = 'US'
  --and adomain in ('king.com', 'playrix.com')
  and (winning_bid_bundle ILIKE '%.playrix.%' OR winning_bid_bundle ILIKE '%.king.%')
  --or winning_bid_bundle ILIKE '%.playrix.%')
group by 1
