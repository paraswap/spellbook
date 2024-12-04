--para-fee-details-with-epoch-v3  https://dune.com/queries/4257927

with fee_claim_detail as (
    
    select 'arbitrum' as blockchain,            
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
            and call_block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20
        
        union all
        
        -- Transfer to SmartVault directly
        select 'arbitrum' as blockchain,            
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
        -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_arbitrum.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
            and t.block_number = erc2.evt_block_number
            and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc2.evt_index > erc.evt_index            
        left join erc20_arbitrum.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
            and t.block_number = erc3.evt_block_number
            and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc3.evt_index > erc.evt_index            
        where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
            and t.success
            and block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20        
        and not exists (
            select 1 from paraswap_arbitrum.FeeClaimer_call_registerFee
            where call_tx_hash = erc.evt_tx_hash
            and call_block_number = erc.evt_block_number
        )

        union all
        
        select 'arbitrum' as blockchain,            
            date_trunc('day', t.block_time) as block_date,
            t.block_time as block_time,
            t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as token_address,
            t.value as fee_raw
        from arbitrum.transactions tr
        join arbitrum.traces t on tr.hash = t.tx_hash
            and tr.block_number = t.block_number        
            and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
        -- If following transfers have outgoing only, exclude this revenue.
        left join arbitrum.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
            and tr.block_number = t2.block_number    
            and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and t2.trace_address > t.trace_address
            and t2.type = 'call'
            and t2.call_type = 'call'
            and t2.value > cast(0 as uint256)
        left join arbitrum.traces t3 on tr.hash = t3.tx_hash -- Outgoing
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
                select 1 from paraswap_arbitrum.FeeClaimer_call_registerFee
                where call_tx_hash = t.tx_hash
                and call_block_number = t.block_number
            )
        
        union all
    
    select 'avalanche_c' as blockchain,            
            date_trunc('day', call_block_time) as block_date,
            call_block_time as block_time,
            call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_avalanche_c.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20
        
        union all
        
        -- Transfer to SmartVault directly
        select 'avalanche_c' as blockchain,            
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
        -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_avalanche_c.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
            and t.block_number = erc2.evt_block_number
            and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc2.evt_index > erc.evt_index            
        left join erc20_avalanche_c.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
            and t.block_number = erc3.evt_block_number
            and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc3.evt_index > erc.evt_index            
        where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
            and t.success
            and block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20        
        and not exists (
            select 1 from paraswap_avalanche_c.FeeClaimer_call_registerFee
            where call_tx_hash = erc.evt_tx_hash
            and call_block_number = erc.evt_block_number
        )

        union all
        
        select 'avalanche_c' as blockchain,            
            date_trunc('day', t.block_time) as block_date,
            t.block_time as block_time,
            t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 as token_address,
            t.value as fee_raw
        from avalanche_c.transactions tr
        join avalanche_c.traces t on tr.hash = t.tx_hash
            and tr.block_number = t.block_number        
            and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
        -- If following transfers have outgoing only, exclude this revenue.
        left join avalanche_c.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
            and tr.block_number = t2.block_number    
            and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and t2.trace_address > t.trace_address
            and t2.type = 'call'
            and t2.call_type = 'call'
            and t2.value > cast(0 as uint256)
        left join avalanche_c.traces t3 on tr.hash = t3.tx_hash -- Outgoing
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
                select 1 from paraswap_avalanche_c.FeeClaimer_call_registerFee
                where call_tx_hash = t.tx_hash
                and call_block_number = t.block_number
            )
        
        union all
    
    select 'bnb' as blockchain,            
            date_trunc('day', call_block_time) as block_date,
            call_block_time as block_time,
            call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c else _token end) as token_address,
            _fee as fee_raw
        from paraswap_bnb.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20
        
        union all
        
        -- Transfer to SmartVault directly
        select 'bnb' as blockchain,            
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
        -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_bnb.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
            and t.block_number = erc2.evt_block_number
            and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc2.evt_index > erc.evt_index            
        left join erc20_bnb.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
            and t.block_number = erc3.evt_block_number
            and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc3.evt_index > erc.evt_index            
        where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
            and t.success
            and block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20        
        and not exists (
            select 1 from paraswap_bnb.FeeClaimer_call_registerFee
            where call_tx_hash = erc.evt_tx_hash
            and call_block_number = erc.evt_block_number
        )

        union all
        
        select 'bnb' as blockchain,            
            date_trunc('day', t.block_time) as block_date,
            t.block_time as block_time,
            t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c as token_address,
            t.value as fee_raw
        from bnb.transactions tr
        join bnb.traces t on tr.hash = t.tx_hash
            and tr.block_number = t.block_number        
            and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
        -- If following transfers have outgoing only, exclude this revenue.
        left join bnb.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
            and tr.block_number = t2.block_number    
            and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and t2.trace_address > t.trace_address
            and t2.type = 'call'
            and t2.call_type = 'call'
            and t2.value > cast(0 as uint256)
        left join bnb.traces t3 on tr.hash = t3.tx_hash -- Outgoing
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
                select 1 from paraswap_bnb.FeeClaimer_call_registerFee
                where call_tx_hash = t.tx_hash
                and call_block_number = t.block_number
            )
        
        union all
    
    select 'ethereum' as blockchain,            
            date_trunc('day', call_block_time) as block_date,
            call_block_time as block_time,
            call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_ethereum.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20
        
        union all
        
        -- Transfer to SmartVault directly
        select 'ethereum' as blockchain,            
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
        -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_ethereum.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
            and t.block_number = erc2.evt_block_number
            and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc2.evt_index > erc.evt_index            
        left join erc20_ethereum.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
            and t.block_number = erc3.evt_block_number
            and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc3.evt_index > erc.evt_index            
        where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
            and t.success
            and block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20        
        and not exists (
            select 1 from paraswap_ethereum.FeeClaimer_call_registerFee
            where call_tx_hash = erc.evt_tx_hash
            and call_block_number = erc.evt_block_number
        )

        union all
        
        select 'ethereum' as blockchain,            
            date_trunc('day', t.block_time) as block_date,
            t.block_time as block_time,
            t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token_address,
            t.value as fee_raw
        from ethereum.transactions tr
        join ethereum.traces t on tr.hash = t.tx_hash
            and tr.block_number = t.block_number        
            and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
        -- If following transfers have outgoing only, exclude this revenue.
        left join ethereum.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
            and tr.block_number = t2.block_number    
            and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and t2.trace_address > t.trace_address
            and t2.type = 'call'
            and t2.call_type = 'call'
            and t2.value > cast(0 as uint256)
        left join ethereum.traces t3 on tr.hash = t3.tx_hash -- Outgoing
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
                select 1 from paraswap_ethereum.FeeClaimer_call_registerFee
                where call_tx_hash = t.tx_hash
                and call_block_number = t.block_number
            )
        
        union all
    
    select 'fantom' as blockchain,            
            date_trunc('day', call_block_time) as block_date,
            call_block_time as block_time,
            call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_fantom.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20
        
        union all
        
        -- Transfer to SmartVault directly
        select 'fantom' as blockchain,            
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
        -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_fantom.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
            and t.block_number = erc2.evt_block_number
            and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc2.evt_index > erc.evt_index            
        left join erc20_fantom.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
            and t.block_number = erc3.evt_block_number
            and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc3.evt_index > erc.evt_index            
        where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
            and t.success
            and block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20        
        and not exists (
            select 1 from paraswap_fantom.FeeClaimer_call_registerFee
            where call_tx_hash = erc.evt_tx_hash
            and call_block_number = erc.evt_block_number
        )

        union all
        
        select 'fantom' as blockchain,            
            date_trunc('day', t.block_time) as block_date,
            t.block_time as block_time,
            t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83 as token_address,
            t.value as fee_raw
        from fantom.transactions tr
        join fantom.traces t on tr.hash = t.tx_hash
            and tr.block_number = t.block_number        
            and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
        -- If following transfers have outgoing only, exclude this revenue.
        left join fantom.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
            and tr.block_number = t2.block_number    
            and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and t2.trace_address > t.trace_address
            and t2.type = 'call'
            and t2.call_type = 'call'
            and t2.value > cast(0 as uint256)
        left join fantom.traces t3 on tr.hash = t3.tx_hash -- Outgoing
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
                select 1 from paraswap_fantom.FeeClaimer_call_registerFee
                where call_tx_hash = t.tx_hash
                and call_block_number = t.block_number
            )
        
        union all
    
    select 'optimism' as blockchain,            
            date_trunc('day', call_block_time) as block_date,
            call_block_time as block_time,
            call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x4200000000000000000000000000000000000006 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_optimism.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20
        
        union all
        
        -- Transfer to SmartVault directly
        select 'optimism' as blockchain,            
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
        -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_optimism.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
            and t.block_number = erc2.evt_block_number
            and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc2.evt_index > erc.evt_index            
        left join erc20_optimism.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
            and t.block_number = erc3.evt_block_number
            and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc3.evt_index > erc.evt_index            
        where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
            and t.success
            and block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20        
        and not exists (
            select 1 from paraswap_optimism.FeeClaimer_call_registerFee
            where call_tx_hash = erc.evt_tx_hash
            and call_block_number = erc.evt_block_number
        )

        union all
        
        select 'optimism' as blockchain,            
            date_trunc('day', t.block_time) as block_date,
            t.block_time as block_time,
            t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0x4200000000000000000000000000000000000006 as token_address,
            t.value as fee_raw
        from optimism.transactions tr
        join optimism.traces t on tr.hash = t.tx_hash
            and tr.block_number = t.block_number        
            and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
        -- If following transfers have outgoing only, exclude this revenue.
        left join optimism.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
            and tr.block_number = t2.block_number    
            and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and t2.trace_address > t.trace_address
            and t2.type = 'call'
            and t2.call_type = 'call'
            and t2.value > cast(0 as uint256)
        left join optimism.traces t3 on tr.hash = t3.tx_hash -- Outgoing
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
                select 1 from paraswap_optimism.FeeClaimer_call_registerFee
                where call_tx_hash = t.tx_hash
                and call_block_number = t.block_number
            )
        
        union all
    
    select 'polygon' as blockchain,            
            date_trunc('day', call_block_time) as block_date,
            call_block_time as block_time,
            call_block_number,
            call_tx_hash,
            _account as user_address,
            (case when _token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 else _token end) as token_address,
            _fee as fee_raw
        from paraswap_polygon.FeeClaimer_call_registerFee
        where call_success = true
            and call_block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20
        
        union all
        
        -- Transfer to SmartVault directly
        select 'polygon' as blockchain,            
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
        -- If following transfers have outgoing only, exclude this revenue.
        left join erc20_polygon.evt_Transfer erc2 on t.hash = erc2.evt_tx_hash
            and t.block_number = erc2.evt_block_number
            and erc2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and erc2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc2.evt_index > erc.evt_index            
        left join erc20_polygon.evt_Transfer erc3 on t.hash = erc3.evt_tx_hash
            and t.block_number = erc3.evt_block_number
            and erc3."from" = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and erc3.evt_index > erc.evt_index            
        where (erc2.evt_tx_hash is not null or erc3.evt_tx_hash is null)
            and t.success
            and block_time >= TIMESTAMP '2024-07-08 12:00' -- Start from Epoch 20        
        and not exists (
            select 1 from paraswap_polygon.FeeClaimer_call_registerFee
            where call_tx_hash = erc.evt_tx_hash
            and call_block_number = erc.evt_block_number
        )

        union all
        
        select 'polygon' as blockchain,            
            date_trunc('day', t.block_time) as block_date,
            t.block_time as block_time,
            t.block_number as call_block_number,
            t.tx_hash as call_tx_hash,
            t.to as user_address,
            0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 as token_address,
            t.value as fee_raw
        from polygon.transactions tr
        join polygon.traces t on tr.hash = t.tx_hash
            and tr.block_number = t.block_number        
            and t."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault    
        -- If following transfers have outgoing only, exclude this revenue.
        left join polygon.traces t2 on tr.hash = t2.tx_hash -- Other income for SmartVault
            and tr.block_number = t2.block_number    
            and t2."from" = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57 -- Router
            and t2.to = 0xd5b927956057075377263aab7f8afc12f85100db -- SmartVault
            and t2.trace_address > t.trace_address
            and t2.type = 'call'
            and t2.call_type = 'call'
            and t2.value > cast(0 as uint256)
        left join polygon.traces t3 on tr.hash = t3.tx_hash -- Outgoing
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