create table public.dim_clients
(
    id            serial
        constraint dim_clients_pk
            primary key,
    taxdome_id    varchar not null
        constraint dim_clients_pk_2
            unique,
    company_name  varchar not null,
    email_address varchar,
    created_at    timestamp default CURRENT_TIMESTAMP not null,
    updated_at    timestamp default CURRENT_TIMESTAMP not null
);

create type link_request_status as enum ('Pending', 'Completed', 'Failed');
create table public.fact_link_requests
(
    id           serial
        constraint fact_link_requests_pk
            primary key,
    link_token   varchar not null,
    client_id    integer not null
        references public.dim_clients (id),
    status       link_request_status default 'Pending' not null,
    error_type   varchar,
    error_code   varchar,
    expiration   timestamp not null,
    completed_at timestamp,
    created_at   timestamp default current_timestamp not null,
    updated_at   timestamp default current_timestamp not null
);

create table public.dim_institutions
(
    id                   serial
        constraint dim_institutions_pk
            primary key,
    plaid_institution_id varchar                             not null
        constraint dim_institutions_pk_2
            unique,
    current_name         varchar                             not null,
    previous_name        varchar,
    created_at           timestamp default CURRENT_TIMESTAMP not null,
    updated_at           timestamp default CURRENT_TIMESTAMP not null
);
create or replace function update_institution_name_scd3(
    p_id integer,
    p_new_name varchar
) returns void as $$
declare
    existing_name varchar;
begin
    -- Fetch the current name for the given id
    select current_name
    into existing_name
    from public.dim_institutions
    where id = p_id;

    -- If the new name is the same as the current name, do nothing
    if existing_name = p_new_name then
        return;
    end if;

    -- Update the current_name and shift the existing name to previous_name
    update public.dim_institutions
    set previous_name = current_name, -- Move current name to previous name
        current_name = p_new_name,   -- Update to the new name
        updated_at = current_timestamp -- Update the timestamp
    where id = p_id;
end;
$$ language plpgsql;

create type item_status as enum ('OK', 'ERROR');
create table public.dim_items
(
    id             serial                                not null
        constraint dim_items_pk
            primary key,
    access_token   varchar
        constraint dim_items_pk_2
            unique,
    institution_id int                                   not null
        constraint dim_items_dim_institutions_id_fk
            references public.dim_institutions (id),
    client_id      int                                   not null
        constraint dim_items_dim_clients_id_fk
            references public.dim_clients (id),
    plaid_item_id  varchar                               not null,
    status         item_status default 'OK'              not null,
    key_name       varchar                               not null,
    key_iv         varchar                               not null,
    is_current     bool        default true              not null,
    created_at     timestamp   default current_timestamp not null,
    updated_at     timestamp   default current_timestamp not null
);
create or replace function update_access_token_scd2(
    p_plaid_item_id varchar,
    p_new_access_token varchar,
    p_key_name varchar,
    p_key_iv varchar
) returns void as $$
begin
    -- Step 1: Mark all existing rows for the item as not current (is_current = false)
    update public.dim_items
    set is_current = false,
        updated_at = current_timestamp
    where plaid_item_id = p_plaid_item_id;

    -- Step 2: Insert a new row using the most recent noncurrent row as the base
    insert into public.dim_items (
        access_token,
        institution_id,
        client_id,
        plaid_item_id,
        key_name,
        key_iv,
        is_current,
        created_at,
        updated_at
    )
    select
        p_new_access_token, -- New access_token
        institution_id,
        client_id,
        plaid_item_id,
        p_key_name, -- New key_name
        p_key_iv, -- New key_iv
        true, -- Mark the new record as current
        current_timestamp, -- Created at
        current_timestamp -- Updated at
    from public.dim_items
    where plaid_item_id = p_plaid_item_id
      and is_current = false
    order by updated_at desc -- Use the most recent noncurrent row
    limit 1;
end;
$$ language plpgsql;


create table public.fact_webhooks
(
    id         serial                              not null
        constraint fact_webhooks_pk
            primary key,
    item_id    int,
    type       varchar                             not null,
    code       varchar                             not null,
    request    jsonb                               not null,
    error      jsonb,
    created_at timestamp default current_timestamp not null
);

create table public.dim_accounts
(
    id               serial
        constraint dim_accounts_pk
            primary key,
    plaid_account_id varchar                             not null
        constraint dim_accounts_pk_2
            unique,
    item_id          integer                             not null
        constraint dim_accounts_dim_items_id_fk
            references public.dim_items,
    name             varchar                             not null,
    type             varchar                             not null,
    subtype          varchar                             not null,
    created_at       timestamp default CURRENT_TIMESTAMP not null
);

create type day_names as enum ('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');
create type month_names as enum ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December');
create table public.dim_date
(
    id           serial      not null
        constraint dim_date_pk
            primary key,
    date         date        not null
        constraint dim_date_pk_2
            unique,
    year         int         not null,
    quarter      int         not null,
    month        int         not null,
    mont_name    month_names not null,
    day          int         not null,
    day_of_week  int         not null,
    day_name     day_names   not null,
    is_weekend   bool        not null,
    week_of_year int         not null,
    constraint valid_day
        check (dim_date.day between 1 and 31),
    constraint valid_day_of_week
        check (dim_date.day_of_week between 1 and 7),
    constraint valid_month
        check (dim_date.month between 1 and 12),
    constraint valid_quarter
        check (dim_date.quarter between 1 and 4)
);
