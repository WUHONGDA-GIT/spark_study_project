package hive

import org.apache.spark.sql.SparkSession

object CreateFunnelDailyTable {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder()
      .appName("SparkSessionApp")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse") //此目录在hdfs上
      .enableHiveSupport()//使用到hive一定要开启这个，idea要想连接hive得在pom中加上hive
      .master("local[*]")
      .getOrCreate()
//    val gg=sparkSession.sql("create database cootek")
    val gs=sparkSession.sql("use cootek")
    val gg=sparkSession.sql(
      """
        |CREATE  TABLE `novel_funnel_d`(
        |`identifier` string,
        |`book_id` int,
        |`app_version` string,
        |`os` string,
        |`ntu_middle_id` string,
        |`is_crs` int,
        |`channel_code` string,c
        |`show_pv` int,
        |`clk_pv` int,
        |`add_pv` int,
        |`begin_pv` int,
        |`deep_pv` int,
        |`read_chapter_count` int,
        |`read_duration` bigint,
        |`trimmed_duration` bigint,
        |`fx_chapter_count` int,
        |`rd_duration` bigint,
        |`lsn_duration` bigint,
        |`rd_chapter_count` int,
        |`lsn_chapter_count` int,
        |`rdlsn_st_min` string,
        |`rd_st_min` string,
        |`lsn_st_min` string)
        |partitioned by(dt string,flavor string,pt string)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |""".stripMargin)
    gs.show
    sparkSession.stop()
  }
}
