package hive

import org.apache.spark.sql.SparkSession
//nohup /opt/module/hin/hive --service metastore -p 9083 >/dev/null &    连接前, 启动集群hive的元数据服务
//服务名 runjar
object ConnectToHiveDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder()
      .appName("SparkSessionApp")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse") //此目录在hdfs上
      .enableHiveSupport()//使用到hive一定要开启这个，idea要想连接hive得在pom中加上hive
      .master("local[2]")
//      .master("yarn")
      .getOrCreate()
    val gg=sparkSession.sql("show tables")
    gg.show
    sparkSession.stop()
  }
}
