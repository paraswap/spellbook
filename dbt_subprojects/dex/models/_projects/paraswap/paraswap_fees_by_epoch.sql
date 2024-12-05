-- https://dune.com/queries/4252179
-- para-fee-detail-v4-materialized-view

with paraswap_fee_address(blockchain, address) as (
    values    
    ('ethereum',0xb1720612d0131839dc489fcf20398ea925282fca ), -- no hits since epoch 20
    ('ethereum',0xd5b927956057075377263aab7f8afc12f85100db ), -- v5 smart vault
    ('ethereum',0x619bbf92fd6ba59893327676b2685a3762a49a33 ), -- no hits since epoch 20
    ('arbitrum',0xd5b927956057075377263aab7f8afc12f85100db ), -- v5 smart vault
    ('arbitrum',0xb9eeb869d6dbfc5988d5adf3f63cd6b77ac9d9fd ),
    ('arbitrum',0x7dA82E75BE36Ab9625B1dd40A5aE5181b43473f3 ),
    ('bnb',0xd5b927956057075377263aab7f8afc12f85100db ), -- v5 smart vault
    ('bnb',0xcbb65ad3e64f404b5411486e15561bfb645ce642 ),
    ('bnb',0x8c1a1D0b6286F35d47a676aB78482f1cf3D749dC ),
    ('avalanche_c',0xd5b927956057075377263aab7f8afc12f85100db ), -- v5 smart vault
    ('avalanche_c',0x1bf4c97384e7bdc609017305edb23fd28c13e76a ),
    ('avalanche_c',0xAFFdeC0FE0B5BBfd725642D87D14c465d25F8dE8 ),
    ('fantom',0xd5b927956057075377263aab7f8afc12f85100db ), -- v5 smart vault
    ('fantom',0xced122f9b99d224675eeda13f0c14639c5633f91 ),
    ('fantom',0x5487683dc3216655D0C8AA31255e2e313b99B477 ),
    ('optimism',0xd5b927956057075377263aab7f8afc12f85100db ), -- v5 smart vault
    ('optimism',0x3b28a6f6291f7e8277751f2911ac49c585d049f6 ),
    ('optimism',0xf93A7F819F83DBfDbC307d4D4f0FE5a208C50318 ),
    ('polygon',0xd5b927956057075377263aab7f8afc12f85100db ), -- v5 smart vault
    ('polygon',0x0c84cd406b8a4e07df9a1b15ef348023a1dcd075 ),
    ('polygon',0xABF832105D7D19E5DEC28D014d5a12579dfa1097 )
),

revenue_detail as (
    select f.en as epoch_num,
        bc as blockchain,
        f.bd as block_date,
        f.bt as block_time,
        (case when a.address is null then 'Partner Fees' else 'ParaSwap Revenue' end) as fee_type,
        case when f.bc = 'arbitrum' and f.th in (0x6a5f7a06a96b738f30b8cb2ab3d4bd9cf9342dd374121a9c6ae9532794d4ab42, 0xbfb570699ee9a033312a9df5c44070273c63703df7eb2b4c60541db36b2b5f00) then 0
            when f.bc = 'bnb' and f.th in (0x3cff5cb1447dc8de854758af9cc60fc45b85aebebed59865e813886f3ba0b308) then 0
            when f.bc = 'ethereum' and f.th in (0x454464d6e1552249f597a73fe301d1a180d533d6b8b29e9cd4c6cd2df46e916a,0x053033102a6099e3ba0c641096a2321986ea48e218a5a4fec847198b2889ba41,0x52d2d525c65e2cf4361d382c3bd9977544055bc4a69235f727ead393a6830672) then 0
            when f.bc = 'polygon' and f.ta in (
                0xf06443e106af722c557634ac48ec52ad76733e1c,
                0xc91c06db0f7bffba61e2a5645cc15686f0a8c828,
                0xcf66eb3d546f0415b368d98a95eaf56ded7aa752,
                0x4fd86e5c455e1b34a835484fdd627fe92ec400ae,
                0x8563cfb8809f0de89ab89b0f884358cffeafd28a,
                0x116cf39a540ae5b5073a89929abc8f0e689f21a3
            ) then 0
            when f.p is null and f.fa >= 100000 then 0 -- Drop price if there is no token price in dune and amount_usd > $100k
            when f.fa >= 1000000 then 0 -- Drop price if amount_usd > $1M
            else f.fa end as fee_amount
        --sum(fa) filter (where bd >= current_date - interval '1' day and bd < current_date) as fee_amount_24_hour,
        --sum(fa) filter (where bd >= current_date - interval '90' day and bd < current_date) as fee_amount_90_day,
        --sum(fa) filter (where bd >= date_trunc('year', now())) as fee_amount_ytd
    from dune.paraswap.result_para_fee_details_with_epoch_v_4 f
    left join paraswap_fee_address a on f.bc = a.blockchain and f.ua = a.address
    
    union all
    
    select f.epoch as epoch_num,
        f.blockchain,
        date_trunc('day', f.block_time)as block_date,
        f.block_time,
        'ParaSwap Revenue' as fee_type,
        f.paraswap_revenue as fee_amount
        --sum(fa) filter (where bd >= current_date - interval '1' day and bd < current_date) as fee_amount_24_hour,
        --sum(fa) filter (where bd >= current_date - interval '90' day and bd < current_date) as fee_amount_90_day,
        --sum(fa) filter (where bd >= date_trunc('year', now())) as fee_amount_ytd
    from dune.paraswap.result_paraswap_metamask_swaps_v_4 f
    
    union all
    
    select f.epoch as epoch_num,
        f.blockchain,
        date_trunc('day', f.block_time)as block_date,
        f.block_time,
        'Partner Fees' as fee_type,
        f.partner_fee as fee_amount
        --sum(fa) filter (where bd >= current_date - interval '1' day and bd < current_date) as fee_amount_24_hour,
        --sum(fa) filter (where bd >= current_date - interval '90' day and bd < current_date) as fee_amount_90_day,
        --sum(fa) filter (where bd >= date_trunc('year', now())) as fee_amount_ytd
    from dune.paraswap.result_paraswap_metamask_swaps_v_4 f
)

select f.epoch_num
,f.blockchain
,f.fee_type
,case when f.epoch_num = 15 and f.blockchain = 'polygon' and f.fee_type = 'ParaSwap Revenue' then cast('123000' as double) 
    else f.fee_amount end as fee_amount
from (
    select epoch_num
    ,blockchain
    ,fee_type
    ,sum(fee_amount) as fee_amount
    from revenue_detail
    group by 1,2,3
) f

union all

select *
from dune.paraswap.dataset_para_fee_detail_before_epoch20