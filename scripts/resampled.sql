
create or replace function resampled(_s text, _t text, out result double precision) as
$func$
begin
execute format('select st_scalex(rast) from %s.%s limit 1',quote_ident(_s),quote_ident(_t)) into result; 
end 
$func$
language plpgsql;

create or replace view raster_resampled as (select r_table_schema as sname,r_table_name as tname,resampled(r_table_schema,r_table_name) as resolution from raster_columns);


