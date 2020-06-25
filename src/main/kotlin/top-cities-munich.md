# Create stream for all events in europe
```
CREATE STREAM meetup_events_europe AS
        SELECT *
                FROM meetup_events_stream 
                WHERE 
                        "GROUP"->country='de' 
                        OR "GROUP"->country='gb'
                        OR "GROUP"->country='fr'
                        OR "GROUP"->country='at'
                        OR "GROUP"->country='be'
                        OR "GROUP"->country='bg'
                        OR "GROUP"->country='hr'
                        OR "GROUP"->country='cy'
                        OR "GROUP"->country='cz'
                        OR "GROUP"->country='dk'
                        OR "GROUP"->country='ee'
                        OR "GROUP"->country='fi'
                        OR "GROUP"->country='gr'
                        OR "GROUP"->country='hu'
                        OR "GROUP"->country='ie'
                        OR "GROUP"->country='it'
                        OR "GROUP"->country='lv'
                        OR "GROUP"->country='lt'
                        OR "GROUP"->country='lu'
                        OR "GROUP"->country='mt'
                        OR "GROUP"->country='nl'
                        OR "GROUP"->country='po'
                        OR "GROUP"->country='pt'
                        OR "GROUP"->country='ro'
                        OR "GROUP"->country='sk'
                        OR "GROUP"->country='si'
                        OR "GROUP"->country='es'
                        OR "GROUP"->country='se';
```

# Aggregate all events in europ per city and count tables
```
CREATE TABLE events_per_city_europe AS SELECT "GROUP"->city as city, COUNT(*) AS count FROM meetup_events_europe GROUP BY "GROUP"->city;
```

# Print  (ORDER BY not immplemented yet so no TOP 10)
```
SELECT * FROM events_per_city_europe EMIT CHANGES;
```