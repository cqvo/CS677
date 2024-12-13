-- Similar to link requests, this table records the fact of when a request has been made
-- We also record when the request has been closed out with completed_at, making it bi-temporal
create table public.fact_report_requests
(
    id serial
        constraint fact_report_requests_pk
            primary key,
    plaid_id varchar not null
        constraint fact_report_requests_pk_2
            unique,
    item_id int not null
        constraint fact_report_requests_fk_dim_items
            references public.dim_items (id),
    completed_at timestamp default current_timestamp,
    created_at timestamp default current_timestamp not null,
    updated_at timestamp default current_timestamp not null,
    report_token varchar not null
        constraint fact_report_requests_pk_3
            unique
);

-- We receive the raw json back from Plaid and store it in this staging table
create table public.staging_report
(
    id serial
        constraint staging_report_pk
            primary key,
    created_at timestamp default current_timestamp not null,
    data jsonb not null
);

-- We can flatten the json enough to be in a much more usable format
select
    data->'report'->>'asset_report_id' as asset_report_id,
    data->'report'->>'client_report_id' as client_report_id,
    cast(data->'report'->>'date_generated' as timestamp) as created_at,
    account->>'account_id' as account_id,
    account->'transactions' as transactions,
    account->'historical_balances' as historical_balances
from staging_report
cross join lateral jsonb_array_elements(data->'report'->'items') as items(item)
cross join lateral jsonb_array_elements(items.item->'accounts') as account
where id = 1;

-- And now we need a place to store it
create table public.dim_reports
(
    id serial
        constraint dim_reports_pk
            primary key,
    plaid_id varchar not null,
    client_report_id varchar not null,
    report_request_id int not null
        constraint dim_reports_fact_report_requests
            references public.fact_report_requests (id),
    account_id varchar not null,
    created_at timestamp default current_timestamp not null,
    transactions jsonb,
    historical_balances jsonb
);

-- Flattening and inserting results in 12 new rows from the json originally collected
insert into public.dim_reports (plaid_id, client_report_id, report_request_id, account_id, created_at, transactions, historical_balances)
select
    data->'report'->>'asset_report_id' as plaid_id,
    data->'report'->>'client_report_id' as client_report_id,
    1 as report_request_id,
    account->>'account_id' as account_id,
    cast(data->'report'->>'date_generated' as timestamp) as created_at,
    account->'transactions' as transactions,
    account->'historical_balances' as historical_balances
from staging_report
cross join lateral jsonb_array_elements(data->'report'->'items') as items(item)
cross join lateral jsonb_array_elements(items.item->'accounts') as account
where id = 1;

-- Get the average transaction amount for each account found in the report
with expanded_transactions as (
    select
        account_id,
        (jsonb_array_elements(transactions)->>'amount')::numeric as amount
    from dim_reports
    where report_request_id = 1
)
select
    account_id,
    avg(amount) as average_transaction_amount
from expanded_transactions
group by account_id;

-- Finding the top 3 accounts by average daily balance
with expanded_balances as (
    select
        account_id,
        (jsonb_array_elements(historical_balances)->>'current')::numeric as balance
    from dim_reports
    where report_request_id = 1
)
select
    account_id,
    avg(balance) as average_historical_balance
from expanded_balances
group by account_id
order by average_historical_balance desc
limit 3;
