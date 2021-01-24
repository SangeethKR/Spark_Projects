package org.san.com

import org.apache.spark.sql.SparkSession

import java.util.Properties

object Sparkbasics {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.bin.dir", "C:\\winutils\\")
    val spark = SparkSession.builder().master("local[*]").appName("sparkbasics").getOrCreate()
    val inputdf = spark.read.format("csv")
                            .option("header", "true")
                            .load("src/inputs/input.csv")
    inputdf.show()
  }
}
