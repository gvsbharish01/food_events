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

    val c = spark.read.format("csv").options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).load(sourceFiles)

    c.show()

  }
}
