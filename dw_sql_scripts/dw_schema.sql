-- schema creation

create schema if not exists dw;

-- Table: dw.dim_date

CREATE TABLE IF NOT EXISTS dw.dim_date
(
    date_id date primary key,
    day smallint,
    month smallint,
    year smallint,
    quarter smallint,
    is_weekend boolean
)

-- Table: dw.dim_category

CREATE TABLE IF NOT EXISTS dw.dim_category
(
    category_sk bigserial primary key,
    category_name text unique,
    created_at timestamp default now()
)

-- Table: dw.dim_customer

CREATE TABLE IF NOT EXISTS dw.dim_customer
(
    customer_sk bigserial primary key,
    customer_id text,
    full_name text,
    username text,
    password text,
    email text,
    phone text,
    full_address text,
    geolocation text,
    start_date timestamp,
    end_date timestamp,
    is_current boolean DEFAULT true,
    data_hash text,
    unique(customer_id, start_date)
)

-- Table: dw.dim_product

CREATE TABLE IF NOT EXISTS dw.dim_product
(
    product_sk bigserial primary key,
    product_id text unique,
    title text,
    price numeric,
    description text,
    image_url text,
    rating_rate double precision,
    rating_count numeric,
    data_hash text,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now(),
    category_sk bigint references dw.dim_category(category_sk)
)

-- Table: dw.fact_cart

CREATE TABLE IF NOT EXISTS dw.fact_cart
(
    cart_sk bigserial primary key,
    cart_id text unique,
    customer_sk bigint references dw.dim_customer (customer_sk),
    cart_date date,
    total_items integer,
    total_value numeric,
)

-- Table: dw.fact_cart_item

CREATE TABLE IF NOT EXISTS dw.fact_cart_item
(
    cart_item_sk bigserial primary key,
    cart_sk bigint references dw.fact_cart(cart_sk),
    product_sk bigint references dw.dim_product (product_sk),
    quantity integer,
    price numeric,
    total numeric,
    created_at timestamp DEFAULT now(),
)