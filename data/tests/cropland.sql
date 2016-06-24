--
-- PostgreSQL database dump
--

-- Dumped from database version 9.4.4
-- Dumped by pg_dump version 9.5.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET search_path = dssat, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: cropland; Type: TABLE; Schema: dssat; Owner: kandread
--

CREATE TABLE cropland (
    rid integer,
    rast public.raster
);


ALTER TABLE cropland OWNER TO kandread;

--
-- Data for Name: cropland; Type: TABLE DATA; Schema: dssat; Owner: kandread
--

COPY cropland (rid, rast) FROM stdin;
1	010000010068D3EF460F11613F68D3EF460F1161BF529F60D6F68F41402CB3FF094703F23F00000000000000000000000000000000E610000002000200040001000000
\.


--
-- PostgreSQL database dump complete
--

