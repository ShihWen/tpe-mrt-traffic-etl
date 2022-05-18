DROP TABLE IF EXISTS staging_traffic;
CREATE TABLE staging_traffic
(
  date        date,
  hour        smallint,
  entrance    varchar(20),
  exit        varchar(20),
  traffic     int
);

DROP TABLE IF EXISTS staging_station_exit;
CREATE TABLE staging_station_exit
(
  station_id         varchar(20)
  , exit_id          smallint
  , stair            boolean
  , escalator        smallint
  , elevator         boolean
  , station_name_zh  varchar(20)
  , station_name_en  varchar(50)
  , exit_name_zh     varchar(50)
  , exit_name_en     varchar(50)
  , exit_longitude   DECIMAL(9,6)
  , exit_latitude    DECIMAL(9,6)
  , exit_geohash     varchar(20)
  , version_id       varchar(10)
  , station_join_key varchar(20)
);

DROP TABLE IF EXISTS staging_station;
CREATE TABLE staging_station
(
  station_id              varchar(20)
  , station_address       varchar(200)
  , bike_allow_on_holidy  boolean
  , location_city         varchar(20)
  , location_city_code    varchar(10)
  , station_name_zh       varchar(20)
  , station_name_en       varchar(50)
  , station_longitude     DECIMAL(9,6)
  , station_latitude      DECIMAL(9,6)
  , station_geohash       varchar(20)
  , version_id            varchar(10)
  , station_join_key      varchar(20)
);

DROP TABLE IF EXISTS mrt_station_dim;
CREATE TABLE mrt_station_dim
(
  station_key             integer identity(0,1)
  , station_id            varchar(20)
  , station_address       varchar(200)
  , bike_allow_on_holidy  boolean
  , location_city         varchar(20)
  , location_city_code    varchar(10)
  , station_name_zh       varchar(20)
  , station_name_en       varchar(50)
  , station_longitude     DECIMAL(9,6)
  , station_latitude      DECIMAL(9,6)
  , station_geohash       varchar(20)
  , version_id            varchar(20)
  , station_join_key      varchar(20)
  , update_date           datetime
);

DROP TABLE IF EXISTS mrt_station_exit_dim;
CREATE TABLE mrt_station_exit_dim
(
  exit_key           integer identity(0,1)
  , station_key      integer
  , exit_id          smallint
  , stair            boolean
  , escalator        smallint
  , elevator         boolean
  , exit_name_zh     varchar(50)
  , exit_name_en     varchar(50)
  , exit_longitude   DECIMAL(9,6)
  , exit_latitude    DECIMAL(9,6)
  , exit_geohash     varchar(20)
  , version_id       varchar(20)
  , update_date      datetime
);

DROP TABLE IF EXISTS time_dim;
CREATE TABLE time_dim
(
  time_key       integer identity(0,1)
  , time         datetime
  , year         integer
  , month        smallint
  , day          smallint
  , hour         smallint
  , day_of_week  smallint
  , week_of_year smallint
  , update_date datetime
);

DROP TABLE IF EXISTS mrt_traffic_fact;
CREATE TABLE mrt_traffic_fact
(
  time_key                  integer
  , entrance_station_key    integer
  , exit_station_key        integer
  , traffic                 integer
  , update_date             datetime
);


COPY staging_station
FROM 's3://mrt-traffic/staging-data/mrt_station_v4.csv'
ACCESS_KEY_ID 'AKIAZDOTSS5X2D5Y6LVH'
SECRET_ACCESS_KEY '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'
REGION 'us-west-2'
CSV
IGNOREHEADER 1;    

INSERT INTO mrt_station_dim
(
  station_id
  , station_address
  , bike_allow_on_holidy
  , location_city
  , location_city_code
  , station_name_zh
  , station_name_en
  , station_longitude
  , station_latitude
  , station_geohash
  , version_id
  , station_join_key
  , update_date 
)
SELECT   station_id
  , station_address
  , bike_allow_on_holidy
  , location_city
  , location_city_code
  , station_name_zh
  , station_name_en
  , station_longitude
  , station_latitude
  , station_geohash
  , version_id
  , station_join_key
  , GETDATE()
FROM staging_station;


COPY staging_station_exit
FROM 's3://mrt-traffic/staging-data/mrt_exit_v3.csv'
ACCESS_KEY_ID 'AKIAZDOTSS5X2D5Y6LVH'
SECRET_ACCESS_KEY '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'
REGION 'us-west-2'
CSV
IGNOREHEADER 1;    

INSERT INTO mrt_station_exit_dim
        (
            station_key
            , exit_id
            , stair
            , escalator
            , elevator
            , exit_name_zh
            , exit_name_en
            , exit_longitude
            , exit_latitude
            , exit_geohash
            , version_id
            , update_date
        )
        SELECT B.station_key
               , A.exit_id
               , A.stair
               , A.escalator
               , A.elevator
               , A.exit_name_zh
               , A.exit_name_en
               , A.exit_longitude
               , A.exit_latitude
               , A.exit_geohash
               , A.version_id
               , GETDATE()
        FROM staging_station_exit A
        LEFT JOIN mrt_station_dim B
        ON A.station_id = B.station_id;
        
        DROP TABLE IF EXISTS mrt_transfer_station_dim;
        WITH transfer_station AS
        (
        SELECT station_name_zh
        FROM
        (
        SELECT station_name_zh
        , station_id
        , ROW_NUMBER() OVER(PARTITION BY station_name_zh ORDER BY station_id) AS RN
        FROM mrt_station_dim
        ) X
        WHERE rn > 1
        )
        SELECT station_key
        , station_id
        , station_name_zh
        , station_name_en
        , GETDATE()
        INTO mrt_transfer_station_dim
        FROM mrt_station_dim A
        WHERE EXISTS
        (
        SELECT station_name_zh
        FROM transfer_station B
        WHERE A.station_name_zh = B.station_name_zh
        )
        ORDER BY station_name_zh, station_id;