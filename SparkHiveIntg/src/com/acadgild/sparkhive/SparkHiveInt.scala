package com.acadgild.sparkhive

import org.apache.spark.sql.SparkSession

object SparkHiveInt {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.master("local")
      .appName("spark session example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport().getOrCreate()
    //val listOfDB = sparkSession.sqlContext.sql("show databases")
    //listOfDB.show(8,false)
    println("test");
    val batchId = args(0)
    val set_properties = sparkSession.sqlContext.sql("set hive.auto.convert.join=false")

    val use_project_database = sparkSession.sqlContext.sql("USE project")

    val create_hive_table_top_10_stations = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.top_10_stations" +
      "(" +
      " station_id STRING," +
      " total_distinct_songs_played INT," +
      " distinct_user_count INT" +
      ")" +
      " PARTITIONED BY (batchid INT)" +
      " ROW FORMAT DELIMITED" +
      " FIELDS TERMINATED BY ','" +
      " STORED AS TEXTFILE")

    val insert_into_top_10_stations = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.top_10_stations" +
      s" PARTITION (batchid=$batchId)" +
      " SELECT" +
      " station_id," +
      " COUNT(DISTINCT song_id) AS total_distinct_songs_played," +
      " COUNT(DISTINCT user_id) AS distinct_user_count" +
      " FROM project.enriched_data" +
      " WHERE status='pass'" +
      s" AND (batchid=$batchId)" +
      " AND liked=1" +
      " GROUP BY station_id" +
      " ORDER BY total_distinct_songs_played DESC" +
      " LIMIT 10")

    val create_tale_users_behaviour = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.users_behaviour" +
      " (user_type STRING," +
      " duration INT)" +
      " PARTITIONED BY (batchid INT)" +
      " ROW FORMAT DELIMITED" +
      " FIELDS TERMINATED BY ','" +
      " STORED AS TEXTFILE;")

    val insert_into_users_behaviour = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.users_behaviour" +
      s" PARTITION(batchid=$batchId)" +
      " SELECT" +
      " CASE WHEN (su.user_id IS NULL OR CAST(ed.timestmp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN" + " 'UNSUBSCRIBED'" +
      " WHEN (su.user_id IS NOT NULL AND CAST(ed.timestmp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN" + " 'SUBSCRIBED'" +
      " END AS user_type," +
      " SUM(ABS(CAST(ed.end_ts AS DECIMAL(20,0))-CAST(ed.start_ts AS DECIMAL(20,0)))) AS duration" +
      " FROM enriched_data ed" +
      " LEFT OUTER JOIN subscribed_users su" +
      " ON ed.user_id=su.user_id" +
      " WHERE ed.status='pass'" +
      s" AND ed.batchid=$batchId" +
      " GROUP BY CASE WHEN (su.user_id IS NULL OR CAST(ed.timestmp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0)))" + " THEN 'UNSUBSCRIBED'" +
      " WHEN (su.user_id IS NOT NULL AND CAST(ed.timestmp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN" + " 'SUBSCRIBED' END;")

    val create_table_connected_artists = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.connected_artists" +
      " (artist_id STRING," +
      " user_count INT)" +
      " PARTITIONED BY (batchid INT)" +
      " ROW FORMAT DELIMITED" +
      " FIELDS TERMINATED BY ','" +
      " STORED AS TEXTFILE;")

    val insert_into_connected_artists = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.connected_artists" +
      s" PARTITION(batchid=$batchId)" +
      " SELECT" +
      " ua.artist_id," +
      " COUNT(DISTINCT ua.user_id) AS user_count" +
      " FROM" +
      " (SELECT user_id, artist_id FROM users_artists" +
      " LATERAL VIEW explode(artists_array) artists AS artist_id) ua" +
      " INNER JOIN" +
      " (SELECT artist_id, song_id, user_id" +
      " FROM enriched_data" +
      " WHERE status='pass'" +
      s" AND batchid=$batchId) ed" +
      " ON ua.artist_id=ed.artist_id" +
      " AND ua.user_id=ed.user_id" +
      " GROUP BY ua.artist_id" +
      " ORDER BY user_count DESC" +
      " LIMIT 10;")

    val create_table_royalty_songs = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.top_10_royalty_songs" +
      " (song_id STRING," +
      " duration INT)" +
      " PARTITIONED BY (batchid INT)" +
      " ROW FORMAT DELIMITED" +
      " FIELDS TERMINATED BY ','" +
      " STORED AS TEXTFILE;")

    val insert_into_royalty_songs = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.top_10_royalty_songs" +
      s" PARTITION(batchid=$batchId)" +
      " SELECT song_id," +
      " SUM(ABS(CAST(end_ts AS DECIMAL(20,0))-CAST(start_ts AS DECIMAL(20,0)))) AS duration" +
      " FROM enriched_data" +
      " WHERE status='pass'" +
      s" AND batchid=$batchId" +
      " AND (liked=1 OR song_end_type=0)" +
      " GROUP BY song_id" +
      " ORDER BY duration DESC" +
      " LIMIT 10;")

    val unsubscribed_users = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.top_10_unsubscribed_users" +
      " (user_id STRING," +
      " duration INT)" +
      " PARTITIONED BY (batchid INT)" +
      " ROW FORMAT DELIMITED" +
      " FIELDS TERMINATED BY ','" +
      " STORED AS TEXTFILE;")

    val insert_into_unsubscribed_users = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.top_10_unsubscribed_users" +
      s" PARTITION(batchid=$batchId)" +
      " SELECT" +
      " ed.user_id," +
      " SUM(ABS(CAST(ed.end_ts AS DECIMAL(20,0))-CAST(ed.start_ts AS DECIMAL(20,0)))) AS duration" +
      " FROM enriched_data ed" +
      " LEFT OUTER JOIN subscribed_users su" +
      " ON ed.user_id=su.user_id" +
      " WHERE ed.status='pass'" +
      s" AND ed.batchid=$batchId" +
      " AND (su.user_id IS NULL OR (CAST(ed.timestmp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))))" +
      " GROUP BY ed.user_id" +
      " ORDER BY duration DESC" +
      " LIMIT 10;")
    val show_tables = sparkSession.sqlContext.sql("show tables")
    show_tables.show(10, false)

    sparkSession.sqlContext.sql("SELECT * from project.top_10_stations").show()
    sparkSession.sqlContext.sql("SELECT * from project.users_behaviour").show()
    sparkSession.sqlContext.sql("SELECT * from project.connected_artists").show()
    sparkSession.sqlContext.sql("SELECT * from project.top_10_royalty_songs").show()
    sparkSession.sqlContext.sql("SELECT * from project.top_10_unsubscribed_users").show()
  }
}