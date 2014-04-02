UPDATE src_weather 
SET geom = ST_PointFromText('POINT('||to_char(lon, '99D9999999999999')||to_char(lat, '99D9999999999999')||')', 4326)
WHERE geom is null;
