-- Ensure PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Meteo data source table
CREATE TABLE IF NOT EXISTS public.meteo_data (
    "timestamp" timestamp without time zone,
    station_id character varying(64),
    latitude numeric,
    longitude numeric,
    temperature numeric,
    humidity numeric,
    pressure numeric,
    geom geometry,
    CONSTRAINT pkey_meteo_data PRIMARY KEY ("timestamp", station_id)
);

-- Create CRS:4326 geometry trigger function
CREATE OR REPLACE FUNCTION public.set_geom4326()
RETURNS trigger AS $$
BEGIN
    new.geom = ST_SetSRID(ST_MakePoint(new.latitude, new.longitude),4326);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER trg_meteo_data_geom
BEFORE INSERT OR UPDATE ON public.meteo_data
FOR EACH ROW EXECUTE FUNCTION public.set_geom4326();
