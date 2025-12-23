CREATE DATABASE course_project;

-- =======

CREATE TABLE course_project.dim_date (
    date_id         UInt32,
    full_date       Date,
    year            UInt16,
    quarter         UInt8,
    month           UInt8,
    day             UInt8,
    day_of_week     UInt8,
    is_weekend      UInt8
) ENGINE = MergeTree()
ORDER BY date_id;

CREATE TABLE dim_customer (
    customer_id     UInt32,
    full_name       String,
    birth_date      Date,
    gender          String,
    registration_dt Date,
    customer_group  String
) ENGINE = MergeTree()
ORDER BY customer_id;

CREATE TABLE dim_account (
    account_id      UInt32,
    customer_id     UInt32,
    account_number  String,
    account_type    String,
    open_date       Date,
    close_date      Nullable(Date),
    status          String
) ENGINE = MergeTree()
ORDER BY account_id;

CREATE TABLE dim_branch (
    branch_id       UInt32,
    branch_name     String,
    region_id       UInt32,
    address         String
) ENGINE = MergeTree()
ORDER BY branch_id;

CREATE TABLE dim_currency (
    currency_id     UInt32,
    currency_code   String,
    currency_name   String
) ENGINE = MergeTree()
ORDER BY currency_id;

CREATE TABLE dim_transaction_type (
    transaction_type_id UInt32,
    name                String,
    description         String
) ENGINE = MergeTree()
ORDER BY transaction_type_id;

CREATE TABLE dim_product (
    product_id      UInt32,
    product_name    String,
    product_type    String,
    interest_rate   Float32
) ENGINE = MergeTree()
ORDER BY product_id;

CREATE TABLE dim_employee (
    employee_id     UInt32,
    full_name       String,
    position        String,
    branch_id       UInt32
) ENGINE = MergeTree()
ORDER BY employee_id;

CREATE TABLE dim_region (
    region_id       UInt32,
    region_name     String
) ENGINE = MergeTree()
ORDER BY region_id;

CREATE TABLE dim_channel (
    channel_id      UInt32,
    channel_name    String
) ENGINE = MergeTree()
ORDER BY channel_id;

CREATE TABLE dim_merchant (
    merchant_id     UInt32,
    merchant_name   String,
    merchant_category String
) ENGINE = MergeTree()
ORDER BY merchant_id;

-- ====

CREATE TABLE fact_transactions (
    transaction_id      UInt64,
    date_id             UInt32,
    account_id          UInt32,
    customer_id         UInt32,
    transaction_type_id UInt32,
    amount              Float64,
    currency_id         UInt32,
    channel_id          UInt32,
    branch_id           UInt32,
    employee_id         UInt32,
    merchant_id         Nullable(UInt32)
) ENGINE = MergeTree()
ORDER BY transaction_id;

CREATE TABLE fact_balances (
    balance_id      UInt64,
    date_id         UInt32,
    account_id      UInt32,
    balance_amount  Float64,
    currency_id     UInt32
) ENGINE = MergeTree()
ORDER BY (account_id, date_id);

CREATE TABLE fact_payments (
    payment_id      UInt64,
    date_id         UInt32,
    account_id      UInt32,
    merchant_id     UInt32,
    amount          Float64,
    currency_id     UInt32,
    channel_id      UInt32
) ENGINE = MergeTree()
ORDER BY payment_id;

CREATE TABLE fact_fees (
    fee_id          UInt64,
    transaction_id  UInt64,
    date_id         UInt32,
    account_id      UInt32,
    fee_amount      Float64,
    fee_type        String
) ENGINE = MergeTree()
ORDER BY fee_id;