import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext, sql}

object DataSetDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("Cache")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val users = Seq(Person("aa",3),Person("bb",3)).toDS().toDF()
    users.map(x=>{
      val name = x.getAs[String]("name")
      (name)
    }).show
  }
}
