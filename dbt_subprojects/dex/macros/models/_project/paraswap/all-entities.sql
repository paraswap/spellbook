{% macro paraswap_all_entities() %}
{%
set date_to = "timestamp '{{date_to}}'"
%}
{%
set date_from = "timestamp '{{date_from}}'"
%}
{# 0xace5ae3de4baffc4a45028659c5ee330764e4f53 is testing agent address on staging #}
{%
set delta_configs = [
    ['ethereum', 'delta-v1-single', 'paraswapdelta_ethereum.ParaswapDeltav1_call_settleSwap', 'contract_address', 'call_block_time', 'call_tx_hash', null],
    ['ethereum', 'delta-v1-batch', 'paraswapdelta_ethereum.ParaswapDeltav1_call_safeSettleBatchSwap', 'contract_address', 'call_block_time', 'call_tx_hash', null],

    ['ethereum', 'delta-v2', 'paraswapdelta_ethereum.ParaswapDeltav2_evt_OrderSettled', 'contract_address', 'evt_block_time', 'evt_tx_hash', null],
    ['base', 'delta-v2', 'paraswapdelta_base.ParaswapDeltav2_evt_OrderSettled', 'contract_address', 'evt_block_time', 'evt_tx_hash', 'evt_tx_from <> 0xace5ae3de4baffc4a45028659c5ee330764e4f53'],

    ['ethereum', 'augustus', 'dex_aggregator.trades', 'project_contract_address', 'block_time', 'tx_hash', "project='paraswap' and blockchain='ethereum'"],

    ['polygon', 'augustus', 'dex_aggregator.trades', 'project_contract_address', 'block_time', 'tx_hash', "project='paraswap' and blockchain='polygon'"],
    ['bnb', 'augustus', 'dex_aggregator.trades', 'project_contract_address', 'block_time', 'tx_hash', "project='paraswap' and blockchain='bnb'"],
    ['arbitrum', 'augustus', 'dex_aggregator.trades', 'project_contract_address', 'block_time', 'tx_hash', "project='paraswap' and blockchain='arbitrum'"],
    ['avalanche_c', 'augustus', 'dex_aggregator.trades', 'project_contract_address', 'block_time', 'tx_hash', "project='paraswap' and blockchain='avalanche_c'"],
    ['fantom', 'augustus', 'dex_aggregator.trades', 'project_contract_address', 'block_time', 'tx_hash', "project='paraswap' and blockchain='fantom'"],
    ['optimism', 'augustus', 'dex_aggregator.trades', 'project_contract_address', 'block_time', 'tx_hash', "project='paraswap' and blockchain='optimism'"],
    ['base', 'augustus', 'dex_aggregator.trades', 'project_contract_address', 'block_time', 'tx_hash', "project='paraswap' and blockchain='base'"],


    
    
]

%}  
with entities as (
    {% for blockchain, entity, table, contract_field_name, time_field_name, hash_field_name, conditional in delta_configs %}
        select '{{ entity }}' as entity, '{{ blockchain }}' as blockchain, {{contract_field_name}} as contract_address, {{time_field_name}} as block_time, {{hash_field_name}} as tx_hash from {{ table }}
        where 
            ({{time_field_name}} BETWEEN {{date_from}} AND {{date_to}})
            {% if conditional %}
            AND {{ conditional }}
            {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)
{% endmacro %}