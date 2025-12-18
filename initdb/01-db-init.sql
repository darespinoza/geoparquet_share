-- Ensure PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Meteo data source table
CREATE TABLE IF NOT EXISTS public.sample_meteo_table (
    "timestamp" timestamp without time zone,
    station_id character varying(64),
    latitude numeric,
    longitude numeric,
    temperature numeric,
    humidity numeric,
    presure numeric,
    geom geometry,
    CONSTRAINT pkey_sample_meteo_table PRIMARY KEY ("timestamp", station_id)
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
CREATE TRIGGER trg_sample_meteo_table_geom
BEFORE INSERT OR UPDATE ON public.sample_meteo_table
FOR EACH ROW EXECUTE FUNCTION public.set_geom4326();
