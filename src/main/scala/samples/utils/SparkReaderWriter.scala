package samples.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.internal.util.TriState.False

object SparkReaderWriter {
  def readData(inputPath: String, inputFormat: String, hasHeader : Boolean=false, delimiter: String): DataFrame = {
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    /*inputFormat match {
      case "CSV" => sparkSession.read.csv(inputPath)
      case "parquet" => sparkSession.read.parquet(inputPath)
      case _ => sparkSession.read.csv(inputPath)
    }*/
    if(inputFormat == "CSV")
      sparkSession.read.option("header", true).option("inferSchema", true).option("delimiter", delimiter).csv(inputPath)
    else {
      sparkSession.read.parquet(inputPath)
    }
  }

  def writeData(df: DataFrame, outputPath: String, outputFormat: String, overwrite: Boolean, partitions: Seq[String]) = {
    if(outputFormat == "CSV") {
      if(overwrite)
      df.write.partitionBy(partitions:_*).mode(SaveMode.Overwrite).csv(outputPath)
      else {
        df.write.partitionBy(partitions:_*).csv(outputPath)
      }
    }
    else {
      if(overwrite)
        df.write.partitionBy(partitions:_*).mode(SaveMode.Overwrite).parquet(outputPath)
      else {
        df.write.partitionBy(partitions:_*).parquet(outputPath)
      }
    }
  }


}
