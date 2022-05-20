import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CachePersistCheckpoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("Cache")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("cp")
    sc.setLogLevel("WARN")
    val strings = List("hello scala","hello spark")
    val rdd = sc.makeRDD(strings)
    val flatRdd = rdd.flatMap(_.split(" "))
    val mapRdd = flatRdd.map(word => {
      println("@@@@@@@@@@@")
      (word, 1)
    })



    println(mapRdd.toDebugString)
    //    mapRdd.persist(StorageLevel.DISK_ONLY)
    mapRdd.cache()
//    mapRdd.checkpoint()
    println(mapRdd.toDebugString)

    mapRdd.reduceByKey(_+_).collect().foreach(println)



    println("#############################")
    println(mapRdd.toDebugString)
    mapRdd.groupByKey().collect().foreach(println)

    sc.stop()


//
////第二个application
//    val conf2 = new SparkConf().setMaster("local[1]").setAppName("Cache")
//    val sc2 = new SparkContext(conf)
//    sc2.setCheckpointDir("cp")
//    sc2.setLogLevel("WARN")
//    val strings2 = List("hello scala","hello spark")
//    val rdd2 = sc2.makeRDD(strings2)
//    val flatRdd2 = rdd2.flatMap(_.split(" "))
//    val mapRdd2 = flatRdd2.map(word => {
//      println("@@@@@@@@@@@")
//      (word, 1)
//    })
//
//
//    println(mapRdd2.toDebugString)
//    //    mapRdd.persist(StorageLevel.DISK_ONLY)
//    mapRdd2.checkpoint()
//    println(mapRdd2.toDebugString)
//
//    val reduceRdd2 = mapRdd2.reduceByKey(_+_)
//    reduceRdd2.collect().foreach(println)
//
//
//    println("#############################")
//    println(mapRdd2.toDebugString)
//
//    val groupRdd2 = mapRdd2.groupByKey()
//    groupRdd2.collect().foreach(println)
//
//    sc2.stop()



//    //在另外的application中重复引用cache
//    val conf2 = new SparkConf().setMaster("local[1]").setAppName("Cache")
//    val sc2 = new SparkContext(conf)
//    sc2.setCheckpointDir("cp")
//    sc2.setLogLevel("WARN")
//
//
//    println(mapRdd.toDebugString)
//    //    mapRdd.persist(StorageLevel.DISK_ONLY)
//    //    mapRdd.cache()
//    //    mapRdd.checkpoint()
//    println(mapRdd.toDebugString)
//    mapRdd.reduceByKey(_+_).collect().foreach(println)
//
//
//    println("#############################")
//    println(mapRdd.toDebugString)
//    mapRdd.groupByKey().collect().foreach(println)
  }
}
