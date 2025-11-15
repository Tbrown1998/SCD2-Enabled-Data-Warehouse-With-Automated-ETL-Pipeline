-- PROCEDURE 1: dw.usp_load_dim_date(date, date) SCD Type 0

CREATE OR REPLACE PROCEDURE dw.usp_load_dim_date(
	IN start_date date DEFAULT (CURRENT_DATE - '5 years'::interval),
	IN end_date date DEFAULT (CURRENT_DATE + '5 years'::interval))
LANGUAGE plpgsql
AS $$
DECLARE
    d DATE := start_date;
BEGIN
    WHILE d <= end_date LOOP
        INSERT INTO dw.dim_date (date_id, day, month, year, quarter, is_weekend)
        VALUES (
            d,
            EXTRACT(DAY FROM d),
            EXTRACT(MONTH FROM d),
            EXTRACT(YEAR FROM d),
            EXTRACT(QUARTER FROM d),
            EXTRACT(ISODOW FROM d) IN (6,7)
        )
        ON CONFLICT (date_id) DO NOTHING;
        d := d + INTERVAL '1 day';
    END LOOP;
END;
$$;

-- PROCEDURE 2: dw.usp_upsert_dim_category() SCD Type 1

CREATE OR REPLACE PROCEDURE dw.usp_upsert_dim_category(
	)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dw.dim_category (category_name, created_at)
    SELECT DISTINCT trim(s.category), now()
    FROM staging.stg_products s
    LEFT JOIN dw.dim_category c
        ON trim(lower(s.category)) = trim(lower(c.category_name))
    WHERE c.category_name IS NULL;
END;
$$;

-- PROCEDURE 3: dw.usp_upsert_dim_product() SCD TYPE 1

CREATE OR REPLACE PROCEDURE dw.usp_upsert_dim_product(
	)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Ensure categories are loaded first
    CALL dw.usp_upsert_dim_category();

    -- Upsert products
    INSERT INTO dw.dim_product (
        product_id, title, price, description, category_sk, image_url, rating_rate,
        rating_count, data_hash, created_at, updated_at
    )
    SELECT
        s.id,
        s.title,
        s.price,
        s.description,
        c.category_sk,
        s.image,
        s.rating_rate,
        s.rating_count,
        md5(s.title || '|' || s.category || '|' || s.price),
        now(),
        now()
    FROM staging.stg_products s
    JOIN dw.dim_category c ON trim(lower(s.category)) = trim(lower(c.category_name))
    ON CONFLICT (product_id)
    DO UPDATE SET
        title = EXCLUDED.title,
        price = EXCLUDED.price,
        description = EXCLUDED.description,
        category_sk = EXCLUDED.category_sk,
        image_url = EXCLUDED.image_url,
        rating_rate = EXCLUDED.rating_rate,
        rating_count = EXCLUDED.rating_count,
        data_hash = EXCLUDED.data_hash,
        updated_at = now()
    WHERE dw.dim_product.data_hash IS DISTINCT FROM EXCLUDED.data_hash;
END;
$$;

-- PROCEDURE 4: dw.usp_scd2_customer() TYPE 2

CREATE OR REPLACE PROCEDURE dw.usp_scd2_customer(
	)
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
    v_hash TEXT;
    v_current_hash TEXT;
    v_current_sk BIGINT;
BEGIN
    -- Loop through all staging customers
    FOR r IN 
        SELECT 
            id, 
            CONCAT(name_first, ' ', name_last) AS full_name,  -- FIX: use first + last name
            username, 
            password, 
            email, 
            phone, 
            CONCAT(address_number, ', ', address_street, ', ', address_city, ', ', 'Atlanta, USA') AS full_address,
            CONCAT(address_geolocation_lat, ', ', address_geolocation_long) AS geolocation
        FROM staging.stg_users
    LOOP

        -- Create row fingerprint (hash)
        v_hash := md5(
            r.full_name || '|' || r.username || '|' || r.password || '|' || r.email 
            || '|' || r.phone || '|' || r.full_address || '|' || r.geolocation
        );

        -- Get the current active version of this customer (if any)
        SELECT customer_sk, data_hash
        INTO v_current_sk, v_current_hash
        FROM dw.dim_customer
        WHERE CAST(customer_id AS TEXT) = CAST(r.id AS TEXT) AND is_current = TRUE
        LIMIT 1;

        -- If customer not found, insert new record
        IF NOT FOUND THEN
            INSERT INTO dw.dim_customer (
                customer_id, full_name, username, password,
                email, phone, full_address, geolocation, 
                start_date, is_current, data_hash
            )
            VALUES (
                r.id, r.full_name, r.username, r.password, r.email, r.phone, 
                r.full_address, r.geolocation, 
                now(), TRUE, v_hash
            );

        -- If found and hash differs, close old record and insert new one
        ELSIF v_current_hash <> v_hash THEN
            UPDATE dw.dim_customer
            SET end_date = now(), is_current = FALSE
            WHERE customer_sk = v_current_sk;

            INSERT INTO dw.dim_customer (
                customer_id, full_name, username, password,
                email, phone, full_address, geolocation, 
                start_date, is_current, data_hash
            )
            VALUES (
                r.id, r.full_name, r.username, r.password, r.email, r.phone, 
                r.full_address, r.geolocation, 
                now(), TRUE, v_hash
            );
        END IF;

    END LOOP;
END;
$$;

-- PROCEDURE 5: dw.usp_load_fact_cart()

CREATE OR REPLACE PROCEDURE dw.usp_load_fact_cart(
	)
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
    v_customer_sk BIGINT;
BEGIN
    FOR r IN 
        SELECT DISTINCT c.cart_id,
               c.user_id,
               SUM(p.price * c.quantity) AS total_value,
               SUM(c.quantity) AS total_items,
               c.date::date AS cart_date
        FROM staging.stg_carts as c
		JOIN staging.stg_products as p
		ON cast(c.product_id as text) = cast(p.id as text)
        GROUP BY cart_id, user_id, date
    LOOP
        -- skip duplicates
        IF EXISTS (SELECT 1 FROM dw.fact_cart WHERE cast(cart_id as text) = cast(r.cart_id as text)) THEN
            CONTINUE;
        END IF;

        -- get customer surrogate key
        SELECT customer_sk INTO v_customer_sk
        FROM dw.dim_customer
        WHERE cast(customer_id as text) = cast(r.user_id as text) AND is_current = TRUE;

        -- insert header row
        INSERT INTO dw.fact_cart (cart_id, customer_sk, cart_date, total_items, total_value)
        VALUES (r.cart_id, v_customer_sk, r.cart_date, r.total_items, r.total_value);
    END LOOP;
END;
$$;

-- PROCEDURE 6: dw.usp_load_fact_cart_item()

CREATE OR REPLACE PROCEDURE dw.usp_load_fact_cart_item(
	)
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
    v_cart_sk BIGINT;
    v_product_sk BIGINT;
    v_price NUMERIC;
BEGIN
    FOR r IN SELECT * FROM staging.stg_carts LOOP
        -- find the parent cart header
        SELECT cart_sk 
        INTO v_cart_sk 
        FROM dw.fact_cart 
        WHERE cast(cart_id as text) = cast(r.cart_id as text);

        -- find product surrogate key and price from the product dimension
        SELECT product_sk, price 
        INTO v_product_sk, v_price
        FROM dw.dim_product
        WHERE cast(product_id as text) = cast(r.product_id as text);

        -- insert line item (use product price from dim_product)
        INSERT INTO dw.fact_cart_item (
            cart_sk, 
            product_sk, 
            quantity, 
            price, 
            total
        )
        VALUES (
            v_cart_sk, 
            v_product_sk, 
            r.quantity, 
            v_price,                -- use price from dim_product
            r.quantity * v_price    -- calculate total dynamically
        );
    END LOOP;
END;
$$;