# Event Processing Cours Project
# Quickstart
## Start Kafka
Start docker conatiners with `docker-compose up -d`

## Produce Events
Build the project and run as a producer

## Create Streams
Connect to ksqldb-cli
`docker exec -it ksqldb-cli ksql http://ksqldb-server:8088`
inside:
`set 'auto.offset.reset' = 'earliest';`

Meetup Event Stream needs to know the schema of the json data:
```
CREATE STREAM meetup_events_stream( 
  utc_offset BIGINT,
  venue STRUCT<
    country VARCHAR,
    city VARCHAR,
    address_1 VARCHAR,
    name VARCHAR,
    lon DOUBLE,
    lat DOUBLE
  >,
  rsvp_limit INTEGER,
  venue_visibility VARCHAR,
  visibility VARCHAR,
  maybe_rsvp_count INTEGER,
  description VARCHAR,
  mtime BIGINT,
  event_url VARCHAR,
  yes_rsvp_count INTEGER,
  payment_required INTEGER,
  name VARCHAR,
  id VARCHAR,
  time BIGINT,
  "GROUP" STRUCT<
    join_mode VARCHAR,
    country VARCHAR,
    city VARCHAR,
    name VARCHAR,
    group_lon DOUBLE,
    id BIGINT,
    state VARCHAR,
    urlname VARCHAR,
    category STRUCT<
      name VARCHAR,
      id VARCHAR,
      shortname VARCHAR
    >,
    group_photo STRUCT<
      highres_link VARCHAR,
      photo_link VARCHAR,
      photo_id BIGINT,
      thumb_link VARCHAR
    >,
    group_lat DOUBLE
  >,
  status VARCHAR) WITH (KAFKA_TOPIC='event_test', VALUE_FORMAT='JSON');
```
Stream to filter events in germany
```
CREATE STREAM meetup_events_stream_de AS SELECT * FROM meetup_events_stream 
WHERE "GROUP"->country = 'de';
```
Stream to filter events in munich
```
CREATE STREAM meetup_events_stream_de_munich AS SELECT * 
FROM meetup_events_stream_de 
WHERE "GROUP"->city = 'Munich' OR "GROUP"->city = 'München';
```
# Create Heatmap
Stream with event id, name and venue lat and lon
```
CREATE STREAM meetup_events_stream_de_munich_ll AS 
SELECT id, name, venue->lon AS lon, venue->lat AS lat
FROM meetup_events_stream_de_munich;
```
In line 107 of index.html there need to be the String: "insert here"

next run the main function of HeatMapMunich.kt

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