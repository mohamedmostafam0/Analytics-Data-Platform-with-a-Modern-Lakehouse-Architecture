CREATE USER etluser WITH PASSWORD 'etlpassword';

CREATE ROLE readonly;

DO $$
BEGIN
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO readonly', current_database());
END
$$;
grant usage on schema public to readonly;
grant select on all tables in schema public to readonly;


alter default privileges in schema public grant select on tables to readonly; 

grant readonly to etluser;

create table if not exists users(
    id serial primary key, 
    first_name varchar(100), 
    last_name varchar(100),
    email varchar(255),
    created_at timestamp default current_timestamp, 
    updated_at timestamp default current_timestamp
);

create table if not exists items(
    id serial primary key, 
    name varchar(100), 
    category varchar(100), 
    price decimal(7,2), 
    inventory int, 
    created_at timestamp default current_timestamp, 
    updated_at timestamp default current_timestamp
);

create table if not exists purchases(
    id serial primary key, 
    user_id bigint references users(id), 
    item_id bigint references items(id), 
    quantity int default 1, 
    purchase_price decimal(12,2), 
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

grant select on table users to readonly; 
grant select on table items to readonly; 
grant select on table purchases to readonly;