-- Runs once, the first time the postgres volume is initialised.
CREATE USER taxi WITH PASSWORD 'taxi';
CREATE DATABASE taxi_dw OWNER taxi;
GRANT ALL PRIVILEGES ON DATABASE taxi_dw TO taxi;
