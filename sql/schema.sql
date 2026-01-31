-- Simple schema for sample table
CREATE TABLE IF NOT EXISTS sample (
    id serial PRIMARY KEY,
    data jsonb,
    created_at timestamptz DEFAULT now()
);
-- =========================================
-- Schema: Car Sales Analytics (PostgreSQL)
-- =========================================

-- 1) Reference table: cars
CREATE TABLE IF NOT EXISTS cars (
    car_id        TEXT PRIMARY KEY,
    brand         TEXT NOT NULL,
    model         TEXT NOT NULL,
    model_year    INT,
    category      TEXT
);

-- 2) Reference table: dealers
CREATE TABLE IF NOT EXISTS dealers (
    dealer_id     TEXT PRIMARY KEY,
    dealer_name   TEXT,
    city          TEXT,
    country       TEXT
);

-- 3) Fact table: sales (transactional)
CREATE TABLE IF NOT EXISTS sales (
    sale_id       TEXT PRIMARY KEY,
    car_id        TEXT NOT NULL REFERENCES cars(car_id) ON UPDATE CASCADE,
    dealer_id     TEXT NOT NULL REFERENCES dealers(dealer_id) ON UPDATE CASCADE,
    sale_date     DATE NOT NULL,
    quantity      INT NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    total_amount  NUMERIC(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);

-- Helpful indexes for analytics queries
CREATE INDEX IF NOT EXISTS idx_sales_sale_date ON sales (sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_car_id ON sales (car_id);
CREATE INDEX IF NOT EXISTS idx_sales_dealer_id ON sales (dealer_id);
