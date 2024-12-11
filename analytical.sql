-- Query to find the number of clients with active items
select count(distinct client_id) as active_clients_count
from public.dim_items
where is_current = true;

-- Query to find institutions with the most accounts
select di.current_name, count(da.id) as account_count
from public.dim_institutions di
join public.dim_items dii on di.id = dii.institution_id
join public.dim_accounts da on dii.id = da.item_id
group by di.id, di.plaid_institution_id, di.current_name
order by account_count desc
limit 10;

-- Query to find items due for key rotation
select di.id, di.key_name
from public.dim_items di
where di.updated_at < current_date - interval '30 days';

-- Query to find average age of items, i.e. how good are we at staying on top of rotation?
select
    avg(current_date - updated_at) as average_item_age_in_days
from
    public.dim_items;
