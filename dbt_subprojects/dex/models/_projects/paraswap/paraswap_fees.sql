--para-fee-details-with-epoch-v3  https://dune.com/queries/4257927
{{ config(
	    schema='paraswap',
        alias = 'fees',
        post_hook='{{ expose_spells(\'["avalanche_c","fantom","arbitrum","bnb","ethereum","optimism","polygon","base"]\',
                                "project",
                                "paraswap",
                                \'["eptighte"]\') }}'
        )
}}{% 

set blockchains = [
    'arbitrum',
    'avalanche_c',
    'bnb',
    'ethereum',
    'fantom',
    'optimism',
    'polygon',
]
%}{%
set blockchain_dependencies = {
    'nativeToken': '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
    'ethereum': {
        'registerFeesV6': 'paraswap_v6_ethereum.AugustusFeeVault_call_registerFees',
        'registerFeesV5': 'paraswap_ethereum.FeeClaimer_call_registerFee',
        'transactions': 'ethereum.transactions',
        'traces': 'ethereum.traces',
        'erc20EvtTransfer': 'erc20_ethereum.evt_Transfer',
        'tokensToReplace': [],
        'wrappedNative': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    },
    'arbitrum': {
        'registerFeesV6': 'paraswap_v6_arbitrum.AugustusFeeVault_call_registerFees',
        'registerFeesV5': 'paraswap_arbitrum.FeeClaimer_call_registerFee',
        'transactions': 'arbitrum.transactions',
        'traces': 'arbitrum.traces',
        'erc20EvtTransfer': 'erc20_arbitrum.evt_Transfer',
        'tokensToReplace': [
            ['0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8', '-- aArbWETH']
        ],
        'wrappedNative': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
    },
    'avalanche_c': {
        'registerFeesV6': 'paraswap_v6_avalanche_c.AugustusFeeVault_call_registerFees',
        'registerFeesV5': 'paraswap_avalanche_c.FeeClaimer_call_registerFee',
        'transactions': 'avalanche_c.transactions',
        'traces': 'avalanche_c.traces',
        'erc20EvtTransfer': 'erc20_avalanche_c.evt_Transfer',
        'tokensToReplace': [],
        'wrappedNative': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
    },
    'bnb': {
        'registerFeesV6': 'paraswap_v6_bnb.AugustusFeeVault_call_registerFees',
        'registerFeesV5': 'paraswap_bnb.FeeClaimer_call_registerFee',
        'transactions': 'bnb.transactions',
        'traces': 'bnb.traces',
        'erc20EvtTransfer': 'erc20_bnb.evt_Transfer',
        'tokensToReplace': [],
        'wrappedNative': '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
    },
    'fantom': {
        'registerFeesV6': 'paraswap_v6_fantom.AugustusFeeVault_call_registerFees',
        'registerFeesV5': 'paraswap_fantom.FeeClaimer_call_registerFee',
        'transactions': 'fantom.transactions',
        'traces': 'fantom.traces',
        'erc20EvtTransfer': 'erc20_fantom.evt_Transfer',
        'tokensToReplace': [],
        'wrappedNative': '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83'
    },
    'optimism': {
        'registerFeesV6': 'paraswap_v6_optimism.AugustusFeeVault_call_registerFees',
        'registerFeesV5': 'paraswap_optimism.FeeClaimer_call_registerFee',
        'transactions': 'optimism.transactions',
        'traces': 'optimism.traces',
        'erc20EvtTransfer': 'erc20_optimism.evt_Transfer',
        'tokensToReplace': [],
        'wrappedNative': '0x4200000000000000000000000000000000000006'
    },
    'polygon': {
        'registerFeesV6': 'paraswap_v6_polygon.AugustusFeeVault_call_registerFees',
        'registerFeesV5': 'paraswap_polygon.FeeClaimer_call_registerFee',
        'transactions': 'polygon.transactions',
        'traces': 'polygon.traces',
        'erc20EvtTransfer': 'erc20_polygon.evt_Transfer',
        'tokensToReplace': [],
        'wrappedNative': '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
    },
    'base': {
        'registerFeesV6': 'paraswap_v6_base.AugustusFeeVault_call_registerFees',
        'transactions': 'base.transactions',
        'traces': 'base.traces',
        'erc20EvtTransfer': 'erc20_base.evt_Transfer',
        'tokensToReplace': [],
        'wrappedNative': '0x4200000000000000000000000000000000000006'
    },
    'zkevm': {
        'registerFeesV6': 'paraswap_v6_zkevm.AugustusFeeVault_call_registerFees',
        'transactions': 'zkevm.transactions',
        'traces': 'zkevm.traces',
        'erc20EvtTransfer': 'erc20_zkevm.evt_Transfer',
        'tokensToReplace': [],
        'wrappedNative': '0x4F9A0e7FD2Bf6067db6994CF12E4495Df938E6e9'
    },
} 
%}
with fee_claim_detail as (
    {% for blockchain in blockchains %}
    select '{{ blockchain }}' as blockchain,            
            date_trunc('day', call_block_time) as block_date,
            call_block_time as block_time,
            call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = {{ blockchain_dependencies.get('nativeToken') }} then {{ blockchain_dependencies[blockchain].get('wrappedNative') }}{% 
            if blockchain_dependencies[blockchain].get('tokensToReplace')|length != 0 %}{% 
                for tokenToReplace in blockchain_dependencies[blockchain].get('tokensToReplace') %}
                when _token = {{ tokenToReplace[0] }} then {{ blockchain_dependencies[blockchain].get('wrappedNative') }} {{ tokenToReplace[1] }}{% 
                endfor %}
                {% 
            endif %} else _token end) as token_address,
            _fee as fee_raw
        from {{ blockchain_dependencies[blockchain].get('registerFeesV5') }}
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20
        
        union all
        
        -- Transfer to SmartVault directly
        select '{{ blockchain }}' as blockchain,            
            date_trunc('day', erc.evt_block_time) as block_date,
            erc.evt_block_time as block_time,
            erc.evt_block_number as call_block_number,
            erc.evt_tx_hash as call_tx_hash,
            erc.to as user_address,{% 
            if blockchain_dependencies[blockchain].get('tokensToReplace')|length == 0 %}
            erc.contract_address as token_address,{% 
                else 
                %}
            (case {% 
                    for 
                        tokenToReplace in blockchain_dependencies[blockchain].get('tokensToReplace') 
                        %}when erc.contract_address = {{ tokenToReplace[0] }} then {{ blockchain_dependencies[blockchain].get('wrappedNative') }} {{ tokenToReplace[1] }}{% 
                    endfor %}
                    else erc.contract_address end) as token_address,{% 
            endif %}
            erc.value as fee_raw
        from {{ blockchain_dependencies[blockchain].get('transactions') }} t
        join {{ blockchain_dependencies[blockchain].get('erc20EvtTransfer') }} erc on t.hash = erc.evt_tx_hash
            and t.block_number = erc.evt_block_number
            and erc."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and erc.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault            
        -- If following transfers have outgoing only, exclude this revenue.
        left join {{ blockchain_dependencies[blockchain].get('erc20EvtTransfer') }} erc2 on t.hash = erc2.evt_tx_hash
            and t.block_number = erc2.evt_block_number
            and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc2.evt_index > erc.evt_index            
        left join {{ blockchain_dependencies[blockchain].get('erc20EvtTransfer') }} erc3 on t.hash = erc3.evt_tx_hash
            and t.block_number = erc3.evt_block_number
            and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc3.evt_index > erc.evt_index            
        where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
            and t.success
            and block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20        
        and not exists (
            select 1 from paraswap_{{blockchain}}.FeeClaimer_call_registerFee
            where call_tx_hash = erc.evt_tx_hash
            and call_block_number = erc.evt_block_number
        )

        union all
        
        select '{{ blockchain }}' as blockchain,            
            date_trunc('day', t.block_time) as block_date,
            t.block_time as block_time,
            t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            {{ blockchain_dependencies[blockchain].get('wrappedNative')}} as token_address,
            t.value as fee_raw
        from {{ blockchain_dependencies[blockchain].get('transactions') }} tr
        join {{ blockchain_dependencies[blockchain].get('traces') }} t on tr.hash = t.tx_hash
            and tr.block_number = t.block_number        
            and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
        -- If following transfers have outgoing only, exclude this revenue.
        left join {{ blockchain_dependencies[blockchain].get('traces') }} t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
            and tr.block_number = t2.block_number    
            and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and t2.trace_address > t.trace_address
            and t2.type = 'call'
            and t2.call_type = 'call'
            and t2.value > cast(0 as uint256)
        left join {{ blockchain_dependencies[blockchain].get('traces') }} t3 on tr.hash = t3.tx_hash -- Outgoing
            and tr.block_number = t3.block_number        
            and t3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and t3.trace_address > t.trace_address
            and t3.type = 'call'
            and t3.call_type = 'call'
            and t3.value > cast(0 as uint256)        
        where (t2.tx_hash is not null or t3.tx_hash is null)
            and tr.success
            and tr.block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20
                and not exists (
                select 1 from paraswap_{{blockchain}}.FeeClaimer_call_registerFee
                where call_tx_hash = t.tx_hash
                and call_block_number = t.block_number
            )
        {% 
        if not loop.last %}
        union all{% 
        endif %}
    {% endfor %}
),
    
price_list as (
    select date_trunc('day', minute) as block_date,
        blockchain,
        contract_address,
        avg(price) as price
    from prices.usd
    where minute >= date('2021-04-01')
    group by 1, 2, 3
)

select e.epoch_num as en,
    f.blockchain as bc,
    f.block_date as bd,
    f.block_time as bt,
    f.call_block_number as bn,
    f.call_tx_hash as th,
    f.user_address as ua,
    f.token_address as ta,
    k.symbol as erc20,
    f.fee_raw,
    mt.raw_price as rp,
    p.price as p,
    --f.fee_raw / power(10, k.decimals) * p.price as fa,
    case when f.blockchain = 'polygon' and f.call_tx_hash in (0xc9d07af12ad163cb41b73209a30899c7e712e5ec02c0b7e01e8663c783623592) then 0 -- Skip tx for avoid price impact
        when f.blockchain = 'bnb' and f.token_address in (
            0xf7659cfe53cf2c1135857a17bf491c945fa8b44d
        ) then 0 -- Avoid price impact
        when k.decimals is not null and coalesce(p.price, 0) > 0 then f.fee_raw / power(10, k.decimals) * p.price
        when sc.decimals is not null then f.fee_raw / power(10, sc.decimals) * sc.price
        when mt.raw_price > 0 then f.fee_raw * mt.raw_price
        else f.fee_raw / power(10, usc.decimals) * usc.price end as fa
from fee_claim_detail f
inner join query_2634288 e on f.block_time >= e.epoch_start_date
        and f.block_time <= e.epoch_end_date -- Epoch by timestamp
left join tokens.erc20 k on f.blockchain = k.blockchain and f.token_address = k.contract_address
left join price_list p on f.token_address = p.contract_address and f.blockchain = p.blockchain and f.block_date = p.block_date
left join query_2742198 sc on f.token_address = sc.contract and f.blockchain = sc.blockchain -- stablecoins
left join dune.sixdegree.result_para_token_price_on_dex mt on f.token_address = mt.token_address
    and f.blockchain = mt.blockchain
    and f.block_date = mt.block_date
left join query_2743935 usc on f.token_address = usc.contract and f.blockchain = usc.blockchain -- unstable coins
