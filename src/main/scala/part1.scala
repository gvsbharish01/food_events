import org.apache.spark.sql._
import java.io.File
import java.lang.System._

import org.apache.spark.SparkContext
import java.net.URI
//import java.nio.file.{Files, Path, Paths}

import scala.reflect.io.Path



object part1 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val sourceFiles = "data\\source\\food_entries\\"
    val part1Files = "data\\target\\part1\\"

    //-----------------------------------------------------------------------------
    //-----------------------------------------------------------------------------
    //Part 1 Create aggregate data for food counts

    val c = spark.read.format("csv").options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).load(sourceFiles)
    c.cache()
    c.createOrReplaceTempView("food_events")
    //c.printSchema()
    //c.show()

    //val fe = spark.sql("select count(1) from food_events where cast(entry_date as date) >= '2017-03-01' and cast(entry_date as date) <= '2017-09-01'")
    val fe = spark.sql("select food_id, count(food_id) as entries_count from food_events where cast(entry_date as date) >= '2017-03-01' and cast(entry_date as date) <= '2017-09-01' group by food_id")

    fe.show()

    //fe.write.option("header",true).csv("data\\target\\part1\\part1.csv")

    //fe.repartition(1).write.format("com.databricks.spark.csv").mode("append").save(part1Files)
    //fe.repartition(1).write.mode("append").csv(path=part1Files)
    fe.repartition(1).write.mode("overwrite").option("header","true").csv(path=part1Files)

  }
}
