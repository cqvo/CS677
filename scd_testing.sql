-- Testing SCD 2
select id, plaid_item_id, access_token, is_current from dim_items where client_id = 19;

select update_access_token_scd2(
    'fba716a5-5f45-4afe-b624-e699fedee9d0',
    'even-newer-access-token',
    'even-new-key-name',
    'even-new-key-iv'
);

select * from dim_items where client_id = 19;

-- Testing SCD 3
select * from dim_institutions where id = 3;

select update_institution_name_scd3(3, 'New Institution Name');

select * from dim_institutions where id = 3;

select update_institution_name_scd3(3, 'Even Newer Institution Name');

select * from dim_institutions where id = 3;
