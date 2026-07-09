CREATE SCHEMA IF NOT EXISTS taxi;
SET search_path TO taxi;


CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER     PRIMARY KEY,
    full_date     DATE        NOT NULL UNIQUE,
    day_of_month  SMALLINT    NOT NULL,
    day_of_week   SMALLINT    NOT NULL,
    day_name      VARCHAR(10) NOT NULL,
    week_of_year  SMALLINT    NOT NULL,
    month_number  SMALLINT    NOT NULL,
    month_name    VARCHAR(10) NOT NULL,
    quarter       SMALLINT    NOT NULL,
    year          SMALLINT    NOT NULL,
    is_weekend    BOOLEAN     NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_key    SMALLINT    PRIMARY KEY,
    hour_of_day SMALLINT    NOT NULL,
    hour_label  VARCHAR(8)  NOT NULL,
    am_pm       VARCHAR(2)  NOT NULL,
    day_part    VARCHAR(12) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_key SMALLINT    PRIMARY KEY,
    borough      VARCHAR(40) NOT NULL,
    zone_name    VARCHAR(80) NOT NULL,
    service_zone VARCHAR(40)
);

CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_key SMALLINT    PRIMARY KEY,
    payment_desc     VARCHAR(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key  SMALLINT    PRIMARY KEY,
    vendor_name VARCHAR(60) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_key  SMALLINT    PRIMARY KEY,
    rate_code_desc VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_taxi_trips (
    trip_id              BIGSERIAL PRIMARY KEY,

    pickup_date_key      INTEGER  NOT NULL REFERENCES dim_date(date_key),
    pickup_time_key      SMALLINT NOT NULL REFERENCES dim_time(time_key),
    dropoff_date_key     INTEGER  NOT NULL REFERENCES dim_date(date_key),
    pu_location_key      SMALLINT NOT NULL REFERENCES dim_location(location_key),
    do_location_key      SMALLINT NOT NULL REFERENCES dim_location(location_key),
    vendor_key           SMALLINT NOT NULL REFERENCES dim_vendor(vendor_key),
    payment_type_key     SMALLINT NOT NULL REFERENCES dim_payment_type(payment_type_key),
    rate_code_key        SMALLINT NOT NULL REFERENCES dim_rate_code(rate_code_key),

    pickup_ts            TIMESTAMP NOT NULL,
    dropoff_ts           TIMESTAMP NOT NULL,

    passenger_count      SMALLINT,
    trip_distance_miles  NUMERIC(8,2)  NOT NULL,
    trip_duration_min    NUMERIC(8,2)  NOT NULL,
    fare_amount          NUMERIC(10,2) NOT NULL,
    tip_amount           NUMERIC(10,2) NOT NULL,
    tolls_amount         NUMERIC(10,2) NOT NULL,
    congestion_surcharge NUMERIC(10,2) NOT NULL DEFAULT 0,
    total_amount         NUMERIC(10,2) NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_fact_pickup_date ON fact_taxi_trips (pickup_date_key);
CREATE INDEX IF NOT EXISTS ix_fact_pickup_time ON fact_taxi_trips (pickup_time_key);
CREATE INDEX IF NOT EXISTS ix_fact_payment     ON fact_taxi_trips (payment_type_key);
CREATE INDEX IF NOT EXISTS ix_fact_pu_location ON fact_taxi_trips (pu_location_key);