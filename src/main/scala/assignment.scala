import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.io.File
import java.lang.System._

import org.apache.spark.SparkContext
import java.net.URI
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.types.{FloatType, LongType, TimestampType}
//import java.nio.file.{Files, Path, Paths}

import scala.reflect.io.Path



object assignment {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("part1_ua")
      .getOrCreate()

    val sourceFiles = "data\\source\\food_entries\\"
    val part1Files = "data\\target\\part1\\"
    val part2Files = "data\\target\\part2\\"
    val sourceData = spark.read.format("csv").options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).load(sourceFiles)
    sourceData.cache()

    sourceData.createOrReplaceTempView("food_events")
    //c.printSchema()
    //c.show()

    //-----------------------------------------------------------------------------
    //Part 1 Create aggregate data for food counts
    //-----------------------------------------------------------------------------


    //val fe = spark.sql("select count(1) from food_events where cast(entry_date as date) >= '2017-03-01' and cast(entry_date as date) <= '2017-09-01'")
    val p1ResultDF = spark.sql("select food_id, count(food_id) as entries_count " +
      "from food_events " +
      "where cast(entry_date as date) >= '2017-03-01' and cast(entry_date as date) <= '2017-09-01' " +
      "group by food_id order by entries_count desc ")
    p1ResultDF.show()
    p1ResultDF.repartition(1).write.mode("overwrite").option("header","true").csv(path=part1Files)

    //-----------------------------------------------------------------------------
    //Part 1 End
    //-----------------------------------------------------------------------------

    //-----------------------------------------------------------------------------
    //Part 2 Create weighted score for recent logs
    //-----------------------------------------------------------------------------

    val date= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    val entry_date_max_filter = "2017-09-01 00:00:00:000"

    val filteredData = sourceData.filter(to_date(col("entry_date"),"yyyy-MM-dd") >= "2017-03-01").filter(to_date(col("entry_date"),"yyyy-MM-dd") <= "2017-09-01")
    val maxTS = filteredData.agg(max(col("timestamp"))).take(1)(0).get(0)
    //val maxTS = date.parse(entry_date_max_filter)
    println(maxTS)

    def assignWeight(i:Int) = i match {
      case i if (i <= 10) => 10000.0
      case i if (i >= 11 && i <= 30) => 700.0
      case i if (i >= 31 && i <= 90) => 20.0
      case i if (i >= 91 && i <= 180) => 10.0
      case i if (i >= 181 && i <= 365) => 5.0
      case i if (i > 365) => 1.0
    }

    val sqlfunc = udf(assignWeight _)


    val TsDiff = filteredData
      .withColumn("MaxTS",lit(maxTS))
      .withColumn("diff_seconds",col("MaxTS").cast(LongType) -
        col("timestamp").cast(LongType))
      .withColumn( "diff_days", ceil(col("diff_seconds") / (24D * 3600D) ))
      .withColumn("weight", sqlfunc(col("diff_days")) )



    TsDiff.orderBy(col("weight").desc).show()
    TsDiff.createOrReplaceTempView("food_event_weights")

    val p2ResultDF = spark.sql("select food_id, sum(weight)/100 as weight_score " +
      "from food_event_weights " +
      "where cast(entry_date as date) >= '2017-03-01' and cast(entry_date as date) <= '2017-09-01' " +
      "group by food_id order by weight_score desc")
    p2ResultDF.show()
    p2ResultDF.repartition(1).write.mode("overwrite").option("header","true").csv(path=part2Files)


    val p3ResultDF = spark.sql("select * from food_event_weights where food_id = '7082' ")

    p3ResultDF.show()


  }
}
