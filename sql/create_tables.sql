CREATE TABLE IF NOT EXISTS dim_ride (
    ride_id VARCHAR(50) PRIMARY KEY,
    rideable_type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_station (
    station_id INTEGER PRIMARY KEY,
    station_name VARCHAR(100),
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS fact_rental (
    rental_id serial PRIMARY KEY,
    ride_id VARCHAR(50) REFERENCES dim_ride (ride_id),
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL,
    start_station_id INTEGER REFERENCES dim_station (station_id),
    end_station_id INTEGER REFERENCES dim_station (station_id),
    member_casual VARCHAR(10)
);