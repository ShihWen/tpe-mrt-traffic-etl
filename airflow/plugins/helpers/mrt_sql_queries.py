class MrtSqlQueries:

    insert_time_dim = ("""
        INSERT INTO time_dim
        (
            time
            , year
            , month
            , day
            , hour
            , day_of_week
            , week_of_year
            , update_date
        )
        SELECT DISTINCT DATEADD(hour,hour,date)
                , EXTRACT( YEAR FROM date)
                , EXTRACT( MONTH FROM date)
                , EXTRACT( DAY FROM date)
                , hour
                , DATE_PART(dow, date) 
                , DATE_PART(w, date) 
                , GETDATE()
        FROM staging_traffic A
        WHERE EXTRACT( YEAR FROM A.date) = {year} AND EXTRACT( MONTH FROM A.date) = {month} ;
    """)

    insert_traffic_fact = ("""
        INSERT INTO mrt_traffic_fact
        (
            time_key
            , entrance_station_key
            , exit_station_key
            , traffic
            , update_date
        )
        SELECT B.time_key
            , in_station.station_key AS entrance_station_key 
            , out_station.station_key AS exit_station_key
            , traffic
            , GETDATE()
        FROM staging_traffic A
        LEFT JOIN time_dim B ON (DATEADD(hour,A.hour,date) = B.time)
        LEFT JOIN mrt_station_dim in_station ON (A.entrance = in_station.station_join_key)
        LEFT JOIN mrt_station_dim out_station ON (A.exit = out_station.station_join_key)
        WHERE EXTRACT( YEAR FROM A.date) = {year} AND EXTRACT( MONTH FROM A.date) = {month} 
    """)
    
    check_station_number_dim = ("""
       SELECT COUNT(*)
       FROM mrt_station_dim
    """)

    check_station_number_staging = ("""
       SELECT COUNT(*)
       FROM staging_station
    """)
    
    check_traffic_fact = ("""
    select count(*)
    from
    (
        select C.time
               , D.station_name_zh as entrance_station
               , A.exit_station_key
               , B.station_name_zh as exit_station
               , B.station_id as exit_id
               , A.traffic
        from mrt_traffic_fact A
        left join mrt_station_dim B on A.exit_station_key = B.station_key
        left join mrt_station_dim D on A.entrance_station_key = D.station_key
        left join time_dim C on A.time_key = C.time_key
        where D.station_name_en = 'Songshan Airport'
        and EXTRACT( YEAR FROM C.time ) = {year}
        and EXTRACT( MONTH FROM C.time ) = {month}
        and EXTRACT( DAY FROM C.time ) = {day}
        and EXTRACT( HOUR FROM C.time ) = 12
    ) X
    """)
