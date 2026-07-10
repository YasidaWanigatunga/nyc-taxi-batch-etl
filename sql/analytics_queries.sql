SET search_path TO taxi;

-- Q1: average fare per mile
SELECT SUM(fare_amount) / NULLIF(SUM(trip_distance_miles), 0) AS avg_fare_per_mile
FROM fact_taxi_trips;

-- Q2: peak hours for taxi rides
SELECT t.hour_label, t.day_part, COUNT(*) AS trip_count
FROM fact_taxi_trips f
JOIN dim_time t ON t.time_key = f.pickup_time_key
GROUP BY t.hour_of_day, t.hour_label, t.day_part
ORDER BY trip_count DESC;

-- Q3: total revenue by payment type
SELECT p.payment_desc,
       COUNT(*)            AS trip_count,
       SUM(f.total_amount) AS total_revenue,
       ROUND(100.0 * SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER (), 2) AS pct_of_revenue
FROM fact_taxi_trips f
JOIN dim_payment_type p ON p.payment_type_key = f.payment_type_key
GROUP BY p.payment_desc
ORDER BY total_revenue DESC;

-- ---------------------------------------------------------------------
-- Bonus: top 10 pickup zones by revenue (shows the location dimension works)
-- ---------------------------------------------------------------------
SELECT
    l.borough,
    l.zone_name,
    COUNT(*)            AS trip_count,
    SUM(f.total_amount) AS total_revenue
FROM fact_taxi_trips f
JOIN dim_location l ON l.location_key = f.pu_location_key
GROUP BY l.borough, l.zone_name
ORDER BY total_revenue DESC
LIMIT 10;
