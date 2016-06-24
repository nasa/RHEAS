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

SET search_path = vic, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: soils; Type: TABLE; Schema: vic; Owner: kandread
--

CREATE TABLE soils (
    rid integer,
    id integer,
    line text,
    depths double precision[],
    elev double precision,
    geom public.geometry(Point,4326),
    resolution double precision
);


ALTER TABLE soils OWNER TO kandread;

--
-- Data for Name: soils; Type: TABLE DATA; Schema: vic; Owner: kandread
--

COPY soils (rid, id, line, depths, elev, geom, resolution) FROM stdin;
53990	53990	1 53990 1.125 35.125 0.216675 0.267014 23.982079 0.516997 2.000000 45.477299 45.477299 45.477299 1024.890259 888.989502 888.989502 -999.000000 -999.000000 -999.000000 88.604271 195.098404 58.970646 1970.739990 0.100000 2.193434 1.605344 17.652630 4.000000 19.093050 22.010283 22.010283 0.461663 0.453788 0.453788 1215.189575 1319.256592 1319.256592 2361.727051 2494.051270 2494.051270 2.000000 0.607689 0.589707 0.589707 0.390452 0.397605 0.397605 0.001000 0.000500 1111.335083 0.079409 0.080601 0.080601 1	{0.100000000000000006,2.19343399999999988,1.6053440000000001}	1970.74000000000001	0101000020E61000000000000000904140000000000000F23F	0.25
\.


--
-- PostgreSQL database dump complete
--

