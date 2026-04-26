-- Retail analytics schema.
-- Star-shaped schema centred on `orders` and `order_items`, with `customers`,
-- `products`, and `categories` as conformed dimensions.

DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    customer_id    SERIAL PRIMARY KEY,
    email          TEXT UNIQUE NOT NULL,
    full_name      TEXT NOT NULL,
    country        TEXT NOT NULL,
    signup_date    DATE NOT NULL,
    marketing_opt_in BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE categories (
    category_id    SERIAL PRIMARY KEY,
    name           TEXT UNIQUE NOT NULL
);

CREATE TABLE products (
    product_id     SERIAL PRIMARY KEY,
    sku            TEXT UNIQUE NOT NULL,
    name           TEXT NOT NULL,
    category_id    INT NOT NULL REFERENCES categories(category_id),
    list_price     NUMERIC(10, 2) NOT NULL CHECK (list_price >= 0),
    is_active      BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE orders (
    order_id       SERIAL PRIMARY KEY,
    customer_id    INT NOT NULL REFERENCES customers(customer_id),
    order_date     TIMESTAMP NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('placed','shipped','delivered','cancelled','returned')),
    shipping_cost  NUMERIC(10, 2) NOT NULL DEFAULT 0
);

CREATE TABLE order_items (
    order_item_id  SERIAL PRIMARY KEY,
    order_id       INT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id     INT NOT NULL REFERENCES products(product_id),
    quantity       INT NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    discount       NUMERIC(10, 2) NOT NULL DEFAULT 0 CHECK (discount >= 0)
);

-- Indexes that support analytical queries below.
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_products_category_id ON products(category_id);
