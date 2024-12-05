--para-fee-details-with-epoch-v3  https://dune.com/queries/4257927
{{ config(
	    schema='paraswap',
        alias = 'fees',
        post_hook='{{ expose_spells(\'["avalanche_c","fantom","arbitrum","bnb","ethereum","optimism","polygon","base"]\',
                                "project",
                                "paraswap",
                                \'["eptighte"]\') }}'
        )
}}
-- old date: '2024-07-08 12:00'  -- start of epoch 20
{% 


set cutoff_date = '2024-07-08 12:00'  

%}{% 

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
        'wrappedNative': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        'delta_v2': '0x0000000000bbF5c5Fd284e657F01Bd000933C96D'
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
        'wrappedNative': '0x4200000000000000000000000000000000000006',
        'delta_v2': '0x0000000000bbF5c5Fd284e657F01Bd000933C96D'
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
with
{% for blockchain in blockchains %}{%     
    if blockchain_dependencies[blockchain].get('delta_v2') %}
    -- delta v2 protocol's and partners revenue src data 
    deltav2_fees_balances_raw_{{ blockchain }} as (
            select            
                evt_block_time,
                evt_block_number,
                evt_tx_hash,                       
                destToken as fee_token,
                protocolFee,
                partnerFee            
            from
                paraswapdelta_{{ blockchain }}.ParaswapDeltav2_evt_OrderSettled as evt            
                where 
                {% if is_incremental() %}                    
                    {{ incremental_predicate('evt_block_time') }} AND
                {% endif %}   
                evt_block_time >= TIMESTAMP '{{ cutoff_date }}'     
    ),{% 
    endif %}    
    -- all registerFee calls on v6 Fee Claimer        
    parsed_fee_data_{{ blockchain }} AS (
        SELECT
            contract_address,
            call_success,
            call_tx_hash,
            call_trace_address,
            call_block_time,
            call_block_number,
            CAST(json_parse(feeData) AS MAP<VARCHAR, JSON>) AS fee_json
        FROM
            paraswap_v6_{{ blockchain }}.AugustusFeeVault_call_registerFees
        WHERE
            call_success = true
            and call_block_time >= TIMESTAMP '{{ cutoff_date }}'     
    ),
    unpacked_fee_data_{{ blockchain }} as (
    SELECT
        contract_address,
        call_success,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        call_block_number,
        CAST(fee_json['addresses'] AS ARRAY<VARCHAR>) AS addresses,
        from_hex(CAST(fee_json['token'] AS VARCHAR)) AS _token,
        CAST(fee_json['fees'] AS ARRAY<DOUBLE>) AS fees
    FROM
        parsed_fee_data_{{ blockchain }}
    ),
    exploded_data_{{ blockchain }} AS (
        SELECT 
            call_block_time,
            call_block_number,
            call_tx_hash, 
            from_hex(address) as "address",
            _token, 
            fee
        FROM 
            unpacked_fee_data_{{ blockchain }}
        CROSS JOIN UNNEST(addresses, fees) AS t(address, fee)
    ),
{% endfor %}
fee_claim_detail as (
    {% for blockchain in blockchains %}{% 
    if blockchain_dependencies[blockchain].get('delta_v2') %}
    -- delta v2 protocol's revenue
    select '{{ blockchain }}' as blockchain,
        'delta-v2' as source, 
        date_trunc('day', evt_block_time) as block_date,
        evt_block_time as block_time,
        evt_block_number as call_block_number,
        evt_tx_hash as call_tx_hash, 
        {{ blockchain_dependencies[blockchain].get('delta_v2') }} as user_address, 
        (case when fee_token = {{ blockchain_dependencies.get('nativeToken') }} then {{ blockchain_dependencies[blockchain].get('wrappedNative') }} else fee_token end) as token_address,        
        protocolFee as fee_raw
    from deltav2_fees_balances_raw_{{ blockchain }}
        where evt_block_time >= TIMESTAMP '{{ cutoff_date }}'     
    union all
    -- delta v2 partners revenue
    select '{{ blockchain }}' as blockchain,
        'delta-v2' as source, 
        date_trunc('day', evt_block_time) as block_date,
        evt_block_time as block_time,
        evt_block_number as call_block_number,
        evt_tx_hash as call_tx_hash, 
        -- TODO: not easy to extract partner address here, may need to find a way
        0x0000000000000000000000000000000000000000 as user_address, 
        (case when fee_token = {{ blockchain_dependencies.get('nativeToken') }} then {{ blockchain_dependencies[blockchain].get('wrappedNative') }} else fee_token end) as token_address,        
        partnerFee as fee_raw
    from deltav2_fees_balances_raw_{{ blockchain }}
        where evt_block_time >= TIMESTAMP '{{ cutoff_date }}'     
    union all
    {% endif %}
    -- <v6>
    -- all registerFee calls on v6 Fee Claimer        
    select '{{ blockchain }}' as blockchain,
        'registerFee-v6' as source, 
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
        call_tx_hash, 
        address as user_address, 
        (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 else _token end) as token_address,
        fee as fee_raw
    FROM 
        exploded_data_{{ blockchain }}
        where call_block_time >= TIMESTAMP '{{ cutoff_date }}' 

    union all
    -- ERC20 transfer to v6 Depositor
    select '{{ blockchain }}' as blockchain,
        'erc20-v6' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
        erc.evt_tx_hash as call_tx_hash,
        erc.to as user_address,
        (case when erc.contract_address = 0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8 then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- aArbWETH
            else erc.contract_address end) as token_address,
        erc.value as fee_raw
    from {{ blockchain }}.transactions t
    join erc20_{{ blockchain }}.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3, ParaSwapRepayAdapter -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc.evt_block_time >= TIMESTAMP '{{ cutoff_date }}' 
    -- If following transfers have outgoing only, exclude this revenue.
    left join erc20_{{ blockchain }}.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3 -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '{{ cutoff_date }}' 
    left join erc20_{{ blockchain }}.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '{{ cutoff_date }}' 
    -- i don't understand this conditional. Don't count swaps? But then should omit txs that have ANY outgoing transfer of WETH / ETH, no? 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '{{ cutoff_date }}'     

    union all
    -- v6: ETH Transfer to SmartVault directly
    select '{{ blockchain }}' as blockchain,
        'eth-v6' as source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
        t.tx_hash as call_tx_hash,
        t.to as user_address,
        0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as token_address,
        t.value as fee_raw
    from {{ blockchain }}.transactions tr
    join {{ blockchain }}.traces t on 
        t.block_time >= TIMESTAMP '{{ cutoff_date }}' 
        and tr.hash = t.tx_hash
        and tr.block_number = t.block_number        
        -- and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6    
    -- If following transfers have outgoing only, exclude this revenue.
    left join {{ blockchain }}.traces t2 on
        t2.block_time >= TIMESTAMP '{{ cutoff_date }}' 
        and tr.hash = t2.tx_hash -- Other income for Depositor v6
        and tr.block_number = t2.block_number    
        -- and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
    left join {{ blockchain }}.traces t3 on 
        t3.block_time >= TIMESTAMP '{{ cutoff_date }}' 
        and tr.hash = t3.tx_hash -- Outgoing
        and tr.block_number = t3.block_number        
        and t3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '{{ cutoff_date }}' 

    -- </v6>
    union all

    -- v5 fee claimer.registerFee
    select '{{ blockchain }}' as blockchain,
        'registerFee-v5' as source,
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
            and call_block_time >= TIMESTAMP '{{ cutoff_date }}' 

    union all

    -- Transfer to SmartVault directly
    -- v5: ERC20 Transfer to SmartVault directly
    select '{{ blockchain }}' as blockchain,
        'erc20-v5' as source,
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
        and erc.evt_block_time >= TIMESTAMP '{{ cutoff_date }}' 
    -- If following transfers have outgoing only, exclude this revenue.
        left join {{ blockchain_dependencies[blockchain].get('erc20EvtTransfer') }} erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '{{ cutoff_date }}' 
        left join {{ blockchain_dependencies[blockchain].get('erc20EvtTransfer') }} erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '{{ cutoff_date }}' 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '{{ cutoff_date }}' 
    and not exists (
            select 1 from paraswap_{{blockchain}}.FeeClaimer_call_registerFee
        where call_tx_hash = erc.evt_tx_hash
        and call_block_number = erc.evt_block_number
    )    

    union all
        -- v5: ETH Transfer to SmartVault directly
        select '{{ blockchain }}' as blockchain,
        'eth-v5' AS source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            {{ blockchain_dependencies[blockchain].get('wrappedNative')}} as token_address,
        t.value as fee_raw
        from {{ blockchain_dependencies[blockchain].get('transactions') }} tr
        join {{ blockchain_dependencies[blockchain].get('traces') }} t on tr.hash = t.tx_hash
            and t.block_time >= TIMESTAMP '{{ cutoff_date }}' 
        and tr.block_number = t.block_number        
        and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router        
        and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
    -- If following transfers have outgoing only, exclude this revenue.
    left join {{ blockchain_dependencies[blockchain].get('traces') }} t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
                and t2.block_time >= TIMESTAMP '{{ cutoff_date }}' 
                and tr.block_number = t2.block_number    
        and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
        left join {{ blockchain_dependencies[blockchain].get('traces') }} t3 on tr.hash = t3.tx_hash -- Outgoing
        and t3.block_time >= TIMESTAMP '{{ cutoff_date }}' 
        and tr.block_number = t3.block_number        
        and t3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '{{ cutoff_date }}' 
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
    where minute >= TIMESTAMP '{{ cutoff_date }}' 
    group by 1, 2, 3
)

select 
    f.source as source,
    e.epoch_num as en,
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
