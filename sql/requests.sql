-- Энергетическое среднее (эквивалентный уровень) по выборке 
SELECT
    s.name,
    date_trunc('month', m.ts_utc) AS month,
    10 * log(10, avg(power(10, m.db_level / 10.0))) AS leq_month
FROM
    noise_reading m
    JOIN stations s USING (station_id)
GROUP BY
    s.name,
    month
ORDER BY
    s.name,
    month;

-- Средний дневной / ночной уровень для станции: 
SELECT
    s.name,
    EXTRACT(
        HOUR
        FROM
            m.ts_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul'
    ) :: int AS hour_local,
    10 * log(10, avg(power(10, m.db_level / 10.0))) AS leq_by_hour
FROM
    noise_reading m
    JOIN stations s USING (station_id)
GROUP BY
    s.name,
    hour_local
ORDER BY
    s.name,
    hour_local;

--
UPDATE
    stations s
SET
    geom = ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326)
FROM
    (
        VALUES
            (2, 127.0561, 37.54457),
            (3, 126.9769, 37.56470),
            (4, 127.0111, 37.51280),
            (5, 126.9429, 37.55950)
    ) AS v(id, lon, lat)
WHERE
    s.station_id = v.id;