import org.apache.spark.{SparkConf, SparkContext}

object ClosureDemoSpark {
  def main(args: Array[String]): Unit = {
    //1个executor
    val conf = new SparkConf().setMaster("local[1]").setAppName("Cache")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val strings = List("hello scala","hello spark")
    val rdd = sc.makeRDD(strings)
    val flatRdd = rdd.flatMap(_.split(" "))

    //2个分区
    flatRdd.repartition(2)
    //闭包数据
    var str = "@@@"
    sc.broadcast(str)
    //闭包函数
    val mapRdd = flatRdd.map(word => {
      str = str + "task"
      (word, str)
    }).collect().foreach(println)





    println(str + "driver")
    sc.stop()
  }
}
