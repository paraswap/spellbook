--para-fee-details-with-epoch-v3  https://dune.com/queries/4257927

-- old date: '2024-07-08 12:00' 

with

    -- all registerFee calls on v6 Fee Claimer        
    parsed_fee_data_arbitrum AS (
        SELECT
            contract_address,
            call_success,
            call_tx_hash,
            call_trace_address,
            call_block_time,
            call_block_number,
            CAST(json_parse(feeData) AS MAP<VARCHAR, JSON>) AS fee_json
        FROM
            paraswap_v6_arbitrum.AugustusFeeVault_call_registerFees
        WHERE
            call_success = true
    ),
    unpacked_fee_data_arbitrum as (
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
        parsed_fee_data_arbitrum
    ),
    exploded_data_arbitrum AS (
        SELECT 
            call_block_time,
            call_block_number,
            call_tx_hash, 
            from_hex(address) as "address",
            _token, 
            fee
        FROM 
            unpacked_fee_data_arbitrum
        CROSS JOIN UNNEST(addresses, fees) AS t(address, fee)
    ),

    -- all registerFee calls on v6 Fee Claimer        
    parsed_fee_data_avalanche_c AS (
        SELECT
            contract_address,
            call_success,
            call_tx_hash,
            call_trace_address,
            call_block_time,
            call_block_number,
            CAST(json_parse(feeData) AS MAP<VARCHAR, JSON>) AS fee_json
        FROM
            paraswap_v6_avalanche_c.AugustusFeeVault_call_registerFees
        WHERE
            call_success = true
    ),
    unpacked_fee_data_avalanche_c as (
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
        parsed_fee_data_avalanche_c
    ),
    exploded_data_avalanche_c AS (
        SELECT 
            call_block_time,
            call_block_number,
            call_tx_hash, 
            from_hex(address) as "address",
            _token, 
            fee
        FROM 
            unpacked_fee_data_avalanche_c
        CROSS JOIN UNNEST(addresses, fees) AS t(address, fee)
    ),

    -- all registerFee calls on v6 Fee Claimer        
    parsed_fee_data_bnb AS (
        SELECT
            contract_address,
            call_success,
            call_tx_hash,
            call_trace_address,
            call_block_time,
            call_block_number,
            CAST(json_parse(feeData) AS MAP<VARCHAR, JSON>) AS fee_json
        FROM
            paraswap_v6_bnb.AugustusFeeVault_call_registerFees
        WHERE
            call_success = true
    ),
    unpacked_fee_data_bnb as (
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
        parsed_fee_data_bnb
    ),
    exploded_data_bnb AS (
        SELECT 
            call_block_time,
            call_block_number,
            call_tx_hash, 
            from_hex(address) as "address",
            _token, 
            fee
        FROM 
            unpacked_fee_data_bnb
        CROSS JOIN UNNEST(addresses, fees) AS t(address, fee)
    ),

    -- all registerFee calls on v6 Fee Claimer        
    parsed_fee_data_ethereum AS (
        SELECT
            contract_address,
            call_success,
            call_tx_hash,
            call_trace_address,
            call_block_time,
            call_block_number,
            CAST(json_parse(feeData) AS MAP<VARCHAR, JSON>) AS fee_json
        FROM
            paraswap_v6_ethereum.AugustusFeeVault_call_registerFees
        WHERE
            call_success = true
    ),
    unpacked_fee_data_ethereum as (
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
        parsed_fee_data_ethereum
    ),
    exploded_data_ethereum AS (
        SELECT 
            call_block_time,
            call_block_number,
            call_tx_hash, 
            from_hex(address) as "address",
            _token, 
            fee
        FROM 
            unpacked_fee_data_ethereum
        CROSS JOIN UNNEST(addresses, fees) AS t(address, fee)
    ),

    -- all registerFee calls on v6 Fee Claimer        
    parsed_fee_data_fantom AS (
        SELECT
            contract_address,
            call_success,
            call_tx_hash,
            call_trace_address,
            call_block_time,
            call_block_number,
            CAST(json_parse(feeData) AS MAP<VARCHAR, JSON>) AS fee_json
        FROM
            paraswap_v6_fantom.AugustusFeeVault_call_registerFees
        WHERE
            call_success = true
    ),
    unpacked_fee_data_fantom as (
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
        parsed_fee_data_fantom
    ),
    exploded_data_fantom AS (
        SELECT 
            call_block_time,
            call_block_number,
            call_tx_hash, 
            from_hex(address) as "address",
            _token, 
            fee
        FROM 
            unpacked_fee_data_fantom
        CROSS JOIN UNNEST(addresses, fees) AS t(address, fee)
    ),

    -- all registerFee calls on v6 Fee Claimer        
    parsed_fee_data_optimism AS (
        SELECT
            contract_address,
            call_success,
            call_tx_hash,
            call_trace_address,
            call_block_time,
            call_block_number,
            CAST(json_parse(feeData) AS MAP<VARCHAR, JSON>) AS fee_json
        FROM
            paraswap_v6_optimism.AugustusFeeVault_call_registerFees
        WHERE
            call_success = true
    ),
    unpacked_fee_data_optimism as (
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
        parsed_fee_data_optimism
    ),
    exploded_data_optimism AS (
        SELECT 
            call_block_time,
            call_block_number,
            call_tx_hash, 
            from_hex(address) as "address",
            _token, 
            fee
        FROM 
            unpacked_fee_data_optimism
        CROSS JOIN UNNEST(addresses, fees) AS t(address, fee)
    ),

    -- all registerFee calls on v6 Fee Claimer        
    parsed_fee_data_polygon AS (
        SELECT
            contract_address,
            call_success,
            call_tx_hash,
            call_trace_address,
            call_block_time,
            call_block_number,
            CAST(json_parse(feeData) AS MAP<VARCHAR, JSON>) AS fee_json
        FROM
            paraswap_v6_polygon.AugustusFeeVault_call_registerFees
        WHERE
            call_success = true
    ),
    unpacked_fee_data_polygon as (
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
        parsed_fee_data_polygon
    ),
    exploded_data_polygon AS (
        SELECT 
            call_block_time,
            call_block_number,
            call_tx_hash, 
            from_hex(address) as "address",
            _token, 
            fee
        FROM 
            unpacked_fee_data_polygon
        CROSS JOIN UNNEST(addresses, fees) AS t(address, fee)
    ),

fee_claim_detail as (
    
    -- <v6>
    -- all registerFee calls on v6 Fee Claimer        
    select 'arbitrum' as blockchain,
        'registerFee-v6' as source, 
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
        call_tx_hash, 
        address as user_address, 
        (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 else _token end) as token_address,
        fee as fee_raw
    FROM 
        exploded_data_arbitrum
        where call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all
    -- ERC20 transfer to v6 Depositor
    select 'arbitrum' as blockchain,
        'erc20-v6' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
        erc.evt_tx_hash as call_tx_hash,
        erc.to as user_address,
        (case when erc.contract_address = 0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8 then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- aArbWETH
            else erc.contract_address end) as token_address,
        erc.value as fee_raw
    from arbitrum.transactions t
    join erc20_arbitrum.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3, ParaSwapRepayAdapter -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
    left join erc20_arbitrum.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3 -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    left join erc20_arbitrum.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- i don't understand this conditional. Don't count swaps? But then should omit txs that have ANY outgoing transfer of WETH / ETH, no? 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00'     

    union all
    -- v6: ETH Transfer to SmartVault directly
    select 'arbitrum' as blockchain,
        'eth-v6' as source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
        t.tx_hash as call_tx_hash,
        t.to as user_address,
        0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as token_address,
        t.value as fee_raw
    from arbitrum.transactions tr
    join arbitrum.traces t on 
        t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t.tx_hash
        and tr.block_number = t.block_number        
        -- and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6    
    -- If following transfers have outgoing only, exclude this revenue.
    left join arbitrum.traces t2 on
        t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t2.tx_hash -- Other income for Depositor v6
        and tr.block_number = t2.block_number    
        -- and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
    left join arbitrum.traces t3 on 
        t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t3.tx_hash -- Outgoing
        and tr.block_number = t3.block_number        
        and t3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 

    -- </v6>
    union all

    -- v5 fee claimer.registerFee
    select 'arbitrum' as blockchain,
        'registerFee-v5' as source,
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
                when _token = 0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8 then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- aArbWETH
                 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_arbitrum.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all

    -- Transfer to SmartVault directly
    -- v5: ERC20 Transfer to SmartVault directly
    select 'arbitrum' as blockchain,
        'erc20-v5' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
            erc.evt_tx_hash as call_tx_hash,
            erc.to as user_address,
            (case when erc.contract_address = 0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8 then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- aArbWETH
                    else erc.contract_address end) as token_address,
            erc.value as fee_raw
        from arbitrum.transactions t
        join erc20_arbitrum.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        and erc."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_arbitrum.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
        left join erc20_arbitrum.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
            select 1 from paraswap_arbitrum.FeeClaimer_call_registerFee
        where call_tx_hash = erc.evt_tx_hash
        and call_block_number = erc.evt_block_number
    )    

    union all
        -- v5: ETH Transfer to SmartVault directly
        select 'arbitrum' as blockchain,
        'eth-v5' AS source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as token_address,
        t.value as fee_raw
        from arbitrum.transactions tr
        join arbitrum.traces t on tr.hash = t.tx_hash
            and t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t.block_number        
        and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router        
        and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
    -- If following transfers have outgoing only, exclude this revenue.
    left join arbitrum.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
                and t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
                and tr.block_number = t2.block_number    
        and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
        left join arbitrum.traces t3 on tr.hash = t3.tx_hash -- Outgoing
        and t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t3.block_number        
        and t3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
                select 1 from paraswap_arbitrum.FeeClaimer_call_registerFee
        where call_tx_hash = t.tx_hash
        and call_block_number = t.block_number
    )
        
        union all
    
    -- <v6>
    -- all registerFee calls on v6 Fee Claimer        
    select 'avalanche_c' as blockchain,
        'registerFee-v6' as source, 
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
        call_tx_hash, 
        address as user_address, 
        (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 else _token end) as token_address,
        fee as fee_raw
    FROM 
        exploded_data_avalanche_c
        where call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all
    -- ERC20 transfer to v6 Depositor
    select 'avalanche_c' as blockchain,
        'erc20-v6' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
        erc.evt_tx_hash as call_tx_hash,
        erc.to as user_address,
        (case when erc.contract_address = 0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8 then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- aArbWETH
            else erc.contract_address end) as token_address,
        erc.value as fee_raw
    from avalanche_c.transactions t
    join erc20_avalanche_c.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3, ParaSwapRepayAdapter -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
    left join erc20_avalanche_c.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3 -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    left join erc20_avalanche_c.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- i don't understand this conditional. Don't count swaps? But then should omit txs that have ANY outgoing transfer of WETH / ETH, no? 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00'     

    union all
    -- v6: ETH Transfer to SmartVault directly
    select 'avalanche_c' as blockchain,
        'eth-v6' as source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
        t.tx_hash as call_tx_hash,
        t.to as user_address,
        0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as token_address,
        t.value as fee_raw
    from avalanche_c.transactions tr
    join avalanche_c.traces t on 
        t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t.tx_hash
        and tr.block_number = t.block_number        
        -- and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6    
    -- If following transfers have outgoing only, exclude this revenue.
    left join avalanche_c.traces t2 on
        t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t2.tx_hash -- Other income for Depositor v6
        and tr.block_number = t2.block_number    
        -- and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
    left join avalanche_c.traces t3 on 
        t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t3.tx_hash -- Outgoing
        and tr.block_number = t3.block_number        
        and t3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 

    -- </v6>
    union all

    -- v5 fee claimer.registerFee
    select 'avalanche_c' as blockchain,
        'registerFee-v5' as source,
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_avalanche_c.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all

    -- Transfer to SmartVault directly
    -- v5: ERC20 Transfer to SmartVault directly
    select 'avalanche_c' as blockchain,
        'erc20-v5' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
            erc.evt_tx_hash as call_tx_hash,
            erc.to as user_address,
            erc.contract_address as token_address,
            erc.value as fee_raw
        from avalanche_c.transactions t
        join erc20_avalanche_c.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        and erc."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_avalanche_c.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
        left join erc20_avalanche_c.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
            select 1 from paraswap_avalanche_c.FeeClaimer_call_registerFee
        where call_tx_hash = erc.evt_tx_hash
        and call_block_number = erc.evt_block_number
    )    

    union all
        -- v5: ETH Transfer to SmartVault directly
        select 'avalanche_c' as blockchain,
        'eth-v5' AS source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 as token_address,
        t.value as fee_raw
        from avalanche_c.transactions tr
        join avalanche_c.traces t on tr.hash = t.tx_hash
            and t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t.block_number        
        and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router        
        and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
    -- If following transfers have outgoing only, exclude this revenue.
    left join avalanche_c.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
                and t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
                and tr.block_number = t2.block_number    
        and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
        left join avalanche_c.traces t3 on tr.hash = t3.tx_hash -- Outgoing
        and t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t3.block_number        
        and t3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
                select 1 from paraswap_avalanche_c.FeeClaimer_call_registerFee
        where call_tx_hash = t.tx_hash
        and call_block_number = t.block_number
    )
        
        union all
    
    -- <v6>
    -- all registerFee calls on v6 Fee Claimer        
    select 'bnb' as blockchain,
        'registerFee-v6' as source, 
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
        call_tx_hash, 
        address as user_address, 
        (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 else _token end) as token_address,
        fee as fee_raw
    FROM 
        exploded_data_bnb
        where call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all
    -- ERC20 transfer to v6 Depositor
    select 'bnb' as blockchain,
        'erc20-v6' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
        erc.evt_tx_hash as call_tx_hash,
        erc.to as user_address,
        (case when erc.contract_address = 0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8 then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- aArbWETH
            else erc.contract_address end) as token_address,
        erc.value as fee_raw
    from bnb.transactions t
    join erc20_bnb.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3, ParaSwapRepayAdapter -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
    left join erc20_bnb.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3 -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    left join erc20_bnb.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- i don't understand this conditional. Don't count swaps? But then should omit txs that have ANY outgoing transfer of WETH / ETH, no? 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00'     

    union all
    -- v6: ETH Transfer to SmartVault directly
    select 'bnb' as blockchain,
        'eth-v6' as source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
        t.tx_hash as call_tx_hash,
        t.to as user_address,
        0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as token_address,
        t.value as fee_raw
    from bnb.transactions tr
    join bnb.traces t on 
        t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t.tx_hash
        and tr.block_number = t.block_number        
        -- and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6    
    -- If following transfers have outgoing only, exclude this revenue.
    left join bnb.traces t2 on
        t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t2.tx_hash -- Other income for Depositor v6
        and tr.block_number = t2.block_number    
        -- and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
    left join bnb.traces t3 on 
        t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t3.tx_hash -- Outgoing
        and tr.block_number = t3.block_number        
        and t3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 

    -- </v6>
    union all

    -- v5 fee claimer.registerFee
    select 'bnb' as blockchain,
        'registerFee-v5' as source,
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c else _token end) as token_address,
            _fee as fee_raw
        from paraswap_bnb.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all

    -- Transfer to SmartVault directly
    -- v5: ERC20 Transfer to SmartVault directly
    select 'bnb' as blockchain,
        'erc20-v5' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
            erc.evt_tx_hash as call_tx_hash,
            erc.to as user_address,
            erc.contract_address as token_address,
            erc.value as fee_raw
        from bnb.transactions t
        join erc20_bnb.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        and erc."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_bnb.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
        left join erc20_bnb.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
            select 1 from paraswap_bnb.FeeClaimer_call_registerFee
        where call_tx_hash = erc.evt_tx_hash
        and call_block_number = erc.evt_block_number
    )    

    union all
        -- v5: ETH Transfer to SmartVault directly
        select 'bnb' as blockchain,
        'eth-v5' AS source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c as token_address,
        t.value as fee_raw
        from bnb.transactions tr
        join bnb.traces t on tr.hash = t.tx_hash
            and t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t.block_number        
        and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router        
        and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
    -- If following transfers have outgoing only, exclude this revenue.
    left join bnb.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
                and t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
                and tr.block_number = t2.block_number    
        and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
        left join bnb.traces t3 on tr.hash = t3.tx_hash -- Outgoing
        and t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t3.block_number        
        and t3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
                select 1 from paraswap_bnb.FeeClaimer_call_registerFee
        where call_tx_hash = t.tx_hash
        and call_block_number = t.block_number
    )
        
        union all
    
    -- <v6>
    -- all registerFee calls on v6 Fee Claimer        
    select 'ethereum' as blockchain,
        'registerFee-v6' as source, 
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
        call_tx_hash, 
        address as user_address, 
        (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 else _token end) as token_address,
        fee as fee_raw
    FROM 
        exploded_data_ethereum
        where call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all
    -- ERC20 transfer to v6 Depositor
    select 'ethereum' as blockchain,
        'erc20-v6' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
        erc.evt_tx_hash as call_tx_hash,
        erc.to as user_address,
        (case when erc.contract_address = 0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8 then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- aArbWETH
            else erc.contract_address end) as token_address,
        erc.value as fee_raw
    from ethereum.transactions t
    join erc20_ethereum.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3, ParaSwapRepayAdapter -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
    left join erc20_ethereum.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3 -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    left join erc20_ethereum.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- i don't understand this conditional. Don't count swaps? But then should omit txs that have ANY outgoing transfer of WETH / ETH, no? 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00'     

    union all
    -- v6: ETH Transfer to SmartVault directly
    select 'ethereum' as blockchain,
        'eth-v6' as source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
        t.tx_hash as call_tx_hash,
        t.to as user_address,
        0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as token_address,
        t.value as fee_raw
    from ethereum.transactions tr
    join ethereum.traces t on 
        t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t.tx_hash
        and tr.block_number = t.block_number        
        -- and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6    
    -- If following transfers have outgoing only, exclude this revenue.
    left join ethereum.traces t2 on
        t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t2.tx_hash -- Other income for Depositor v6
        and tr.block_number = t2.block_number    
        -- and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
    left join ethereum.traces t3 on 
        t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t3.tx_hash -- Outgoing
        and tr.block_number = t3.block_number        
        and t3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 

    -- </v6>
    union all

    -- v5 fee claimer.registerFee
    select 'ethereum' as blockchain,
        'registerFee-v5' as source,
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_ethereum.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all

    -- Transfer to SmartVault directly
    -- v5: ERC20 Transfer to SmartVault directly
    select 'ethereum' as blockchain,
        'erc20-v5' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
            erc.evt_tx_hash as call_tx_hash,
            erc.to as user_address,
            erc.contract_address as token_address,
            erc.value as fee_raw
        from ethereum.transactions t
        join erc20_ethereum.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        and erc."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_ethereum.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
        left join erc20_ethereum.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
            select 1 from paraswap_ethereum.FeeClaimer_call_registerFee
        where call_tx_hash = erc.evt_tx_hash
        and call_block_number = erc.evt_block_number
    )    

    union all
        -- v5: ETH Transfer to SmartVault directly
        select 'ethereum' as blockchain,
        'eth-v5' AS source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token_address,
        t.value as fee_raw
        from ethereum.transactions tr
        join ethereum.traces t on tr.hash = t.tx_hash
            and t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t.block_number        
        and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router        
        and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
    -- If following transfers have outgoing only, exclude this revenue.
    left join ethereum.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
                and t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
                and tr.block_number = t2.block_number    
        and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
        left join ethereum.traces t3 on tr.hash = t3.tx_hash -- Outgoing
        and t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t3.block_number        
        and t3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
                select 1 from paraswap_ethereum.FeeClaimer_call_registerFee
        where call_tx_hash = t.tx_hash
        and call_block_number = t.block_number
    )
        
        union all
    
    -- <v6>
    -- all registerFee calls on v6 Fee Claimer        
    select 'fantom' as blockchain,
        'registerFee-v6' as source, 
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
        call_tx_hash, 
        address as user_address, 
        (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 else _token end) as token_address,
        fee as fee_raw
    FROM 
        exploded_data_fantom
        where call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all
    -- ERC20 transfer to v6 Depositor
    select 'fantom' as blockchain,
        'erc20-v6' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
        erc.evt_tx_hash as call_tx_hash,
        erc.to as user_address,
        (case when erc.contract_address = 0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8 then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- aArbWETH
            else erc.contract_address end) as token_address,
        erc.value as fee_raw
    from fantom.transactions t
    join erc20_fantom.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3, ParaSwapRepayAdapter -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
    left join erc20_fantom.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3 -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    left join erc20_fantom.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- i don't understand this conditional. Don't count swaps? But then should omit txs that have ANY outgoing transfer of WETH / ETH, no? 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00'     

    union all
    -- v6: ETH Transfer to SmartVault directly
    select 'fantom' as blockchain,
        'eth-v6' as source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
        t.tx_hash as call_tx_hash,
        t.to as user_address,
        0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as token_address,
        t.value as fee_raw
    from fantom.transactions tr
    join fantom.traces t on 
        t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t.tx_hash
        and tr.block_number = t.block_number        
        -- and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6    
    -- If following transfers have outgoing only, exclude this revenue.
    left join fantom.traces t2 on
        t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t2.tx_hash -- Other income for Depositor v6
        and tr.block_number = t2.block_number    
        -- and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
    left join fantom.traces t3 on 
        t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t3.tx_hash -- Outgoing
        and tr.block_number = t3.block_number        
        and t3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 

    -- </v6>
    union all

    -- v5 fee claimer.registerFee
    select 'fantom' as blockchain,
        'registerFee-v5' as source,
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_fantom.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all

    -- Transfer to SmartVault directly
    -- v5: ERC20 Transfer to SmartVault directly
    select 'fantom' as blockchain,
        'erc20-v5' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
            erc.evt_tx_hash as call_tx_hash,
            erc.to as user_address,
            erc.contract_address as token_address,
            erc.value as fee_raw
        from fantom.transactions t
        join erc20_fantom.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        and erc."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_fantom.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
        left join erc20_fantom.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
            select 1 from paraswap_fantom.FeeClaimer_call_registerFee
        where call_tx_hash = erc.evt_tx_hash
        and call_block_number = erc.evt_block_number
    )    

    union all
        -- v5: ETH Transfer to SmartVault directly
        select 'fantom' as blockchain,
        'eth-v5' AS source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83 as token_address,
        t.value as fee_raw
        from fantom.transactions tr
        join fantom.traces t on tr.hash = t.tx_hash
            and t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t.block_number        
        and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router        
        and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
    -- If following transfers have outgoing only, exclude this revenue.
    left join fantom.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
                and t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
                and tr.block_number = t2.block_number    
        and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
        left join fantom.traces t3 on tr.hash = t3.tx_hash -- Outgoing
        and t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t3.block_number        
        and t3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
                select 1 from paraswap_fantom.FeeClaimer_call_registerFee
        where call_tx_hash = t.tx_hash
        and call_block_number = t.block_number
    )
        
        union all
    
    -- <v6>
    -- all registerFee calls on v6 Fee Claimer        
    select 'optimism' as blockchain,
        'registerFee-v6' as source, 
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
        call_tx_hash, 
        address as user_address, 
        (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 else _token end) as token_address,
        fee as fee_raw
    FROM 
        exploded_data_optimism
        where call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all
    -- ERC20 transfer to v6 Depositor
    select 'optimism' as blockchain,
        'erc20-v6' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
        erc.evt_tx_hash as call_tx_hash,
        erc.to as user_address,
        (case when erc.contract_address = 0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8 then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- aArbWETH
            else erc.contract_address end) as token_address,
        erc.value as fee_raw
    from optimism.transactions t
    join erc20_optimism.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3, ParaSwapRepayAdapter -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
    left join erc20_optimism.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3 -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    left join erc20_optimism.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- i don't understand this conditional. Don't count swaps? But then should omit txs that have ANY outgoing transfer of WETH / ETH, no? 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00'     

    union all
    -- v6: ETH Transfer to SmartVault directly
    select 'optimism' as blockchain,
        'eth-v6' as source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
        t.tx_hash as call_tx_hash,
        t.to as user_address,
        0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as token_address,
        t.value as fee_raw
    from optimism.transactions tr
    join optimism.traces t on 
        t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t.tx_hash
        and tr.block_number = t.block_number        
        -- and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6    
    -- If following transfers have outgoing only, exclude this revenue.
    left join optimism.traces t2 on
        t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t2.tx_hash -- Other income for Depositor v6
        and tr.block_number = t2.block_number    
        -- and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
    left join optimism.traces t3 on 
        t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t3.tx_hash -- Outgoing
        and tr.block_number = t3.block_number        
        and t3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 

    -- </v6>
    union all

    -- v5 fee claimer.registerFee
    select 'optimism' as blockchain,
        'registerFee-v5' as source,
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x4200000000000000000000000000000000000006 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_optimism.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all

    -- Transfer to SmartVault directly
    -- v5: ERC20 Transfer to SmartVault directly
    select 'optimism' as blockchain,
        'erc20-v5' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
            erc.evt_tx_hash as call_tx_hash,
            erc.to as user_address,
            erc.contract_address as token_address,
            erc.value as fee_raw
        from optimism.transactions t
        join erc20_optimism.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        and erc."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_optimism.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
        left join erc20_optimism.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
            select 1 from paraswap_optimism.FeeClaimer_call_registerFee
        where call_tx_hash = erc.evt_tx_hash
        and call_block_number = erc.evt_block_number
    )    

    union all
        -- v5: ETH Transfer to SmartVault directly
        select 'optimism' as blockchain,
        'eth-v5' AS source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0x4200000000000000000000000000000000000006 as token_address,
        t.value as fee_raw
        from optimism.transactions tr
        join optimism.traces t on tr.hash = t.tx_hash
            and t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t.block_number        
        and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router        
        and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
    -- If following transfers have outgoing only, exclude this revenue.
    left join optimism.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
                and t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
                and tr.block_number = t2.block_number    
        and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
        left join optimism.traces t3 on tr.hash = t3.tx_hash -- Outgoing
        and t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t3.block_number        
        and t3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
                select 1 from paraswap_optimism.FeeClaimer_call_registerFee
        where call_tx_hash = t.tx_hash
        and call_block_number = t.block_number
    )
        
        union all
    
    -- <v6>
    -- all registerFee calls on v6 Fee Claimer        
    select 'polygon' as blockchain,
        'registerFee-v6' as source, 
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
        call_tx_hash, 
        address as user_address, 
        (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 else _token end) as token_address,
        fee as fee_raw
    FROM 
        exploded_data_polygon
        where call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all
    -- ERC20 transfer to v6 Depositor
    select 'polygon' as blockchain,
        'erc20-v6' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
        erc.evt_tx_hash as call_tx_hash,
        erc.to as user_address,
        (case when erc.contract_address = 0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8 then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- aArbWETH
            else erc.contract_address end) as token_address,
        erc.value as fee_raw
    from polygon.transactions t
    join erc20_polygon.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3, ParaSwapRepayAdapter -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
    left join erc20_polygon.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        -- fees come from Augustus v6 but also from ParaSwapDebtSwapAdapterV3 -- no need to restrict then
        -- and erc."from" = 0x6a000f20005980200259b80c5102003040001068 -- Augustus v6
        and erc2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    left join erc20_polygon.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- i don't understand this conditional. Don't count swaps? But then should omit txs that have ANY outgoing transfer of WETH / ETH, no? 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00'     

    union all
    -- v6: ETH Transfer to SmartVault directly
    select 'polygon' as blockchain,
        'eth-v6' as source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
        t.tx_hash as call_tx_hash,
        t.to as user_address,
        0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as token_address,
        t.value as fee_raw
    from polygon.transactions tr
    join polygon.traces t on 
        t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t.tx_hash
        and tr.block_number = t.block_number        
        -- and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6    
    -- If following transfers have outgoing only, exclude this revenue.
    left join polygon.traces t2 on
        t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t2.tx_hash -- Other income for Depositor v6
        and tr.block_number = t2.block_number    
        -- and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
    left join polygon.traces t3 on 
        t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.hash = t3.tx_hash -- Outgoing
        and tr.block_number = t3.block_number        
        and t3."from" = 0x4d5401b9e9dcd7c9097e1df036c3afafc35d604f -- Depositor v6
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 

    -- </v6>
    union all

    -- v5 fee claimer.registerFee
    select 'polygon' as blockchain,
        'registerFee-v5' as source,
        date_trunc('day', call_block_time) as block_date,
        call_block_time as block_time,
        call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_polygon.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-11-30 00:00' 

    union all

    -- Transfer to SmartVault directly
    -- v5: ERC20 Transfer to SmartVault directly
    select 'polygon' as blockchain,
        'erc20-v5' as source,
        date_trunc('day', erc.evt_block_time) as block_date,
        erc.evt_block_time as block_time,
        erc.evt_block_number as call_block_number,
            erc.evt_tx_hash as call_tx_hash,
            erc.to as user_address,
            erc.contract_address as token_address,
            erc.value as fee_raw
        from polygon.transactions t
        join erc20_polygon.evt_Transfer erc on t.hash = erc.evt_tx_hash
        and t.block_number = erc.evt_block_number
        and erc."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_polygon.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
        and t.block_number = erc2.evt_block_number
        and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc2.evt_index > erc.evt_index
        and erc2.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
        left join erc20_polygon.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
        and t.block_number = erc3.evt_block_number
        and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and erc3.evt_index > erc.evt_index
        and erc3.evt_block_time >= TIMESTAMP '2024-11-30 00:00' 
    where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
    and t.success
    and block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
            select 1 from paraswap_polygon.FeeClaimer_call_registerFee
        where call_tx_hash = erc.evt_tx_hash
        and call_block_number = erc.evt_block_number
    )    

    union all
        -- v5: ETH Transfer to SmartVault directly
        select 'polygon' as blockchain,
        'eth-v5' AS source,
        date_trunc('day', t.block_time) as block_date,
        t.block_time as block_time,
        t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 as token_address,
        t.value as fee_raw
        from polygon.transactions tr
        join polygon.traces t on tr.hash = t.tx_hash
            and t.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t.block_number        
        and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router        
        and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
    -- If following transfers have outgoing only, exclude this revenue.
    left join polygon.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
                and t2.block_time >= TIMESTAMP '2024-11-30 00:00' 
                and tr.block_number = t2.block_number    
        and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
        and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t2.trace_address > t.trace_address
        and t2.type = 'call'
        and t2.call_type = 'call'
        and t2.value > cast(0 as uint256)
        left join polygon.traces t3 on tr.hash = t3.tx_hash -- Outgoing
        and t3.block_time >= TIMESTAMP '2024-11-30 00:00' 
        and tr.block_number = t3.block_number        
        and t3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
        and t3.trace_address > t.trace_address
        and t3.type = 'call'
        and t3.call_type = 'call'
        and t3.value > cast(0 as uint256)        
    where (t2.tx_hash is not null or t3.tx_hash is null)
    and tr.success
    and tr.block_time >= TIMESTAMP '2024-11-30 00:00' 
    and not exists (
                select 1 from paraswap_polygon.FeeClaimer_call_registerFee
        where call_tx_hash = t.tx_hash
        and call_block_number = t.block_number
    )
        
    
),
    
price_list as (
    select date_trunc('day', minute) as block_date,
        blockchain,
        contract_address,
        avg(price) as price
    from prices.usd
    where minute >= TIMESTAMP '2024-11-30 00:00' 
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