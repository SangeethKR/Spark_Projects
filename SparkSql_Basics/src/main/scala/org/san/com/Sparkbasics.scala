package org.san.com

import org.apache.spark.sql.SparkSession

import java.util.Properties

object Sparkbasics {

  def main(args: Array[String]): Unit = {

    /*creating spark session object */
    val spark = UtilFunction.sparkSessionObjectCreate("Sparkbasics")

    /*Reading a csv file to Dataframe with default Reading mode(PERMISSIVE) */
    val readpermissivedf = spark.read.format("csv")
                            .option("header", "true")
                            .option("mode", "PERMISSIVE")
                            .load("src/inputs/input.csv")
    readpermissivedf.show()

    /*Reading Data with read mode(DROPMALFORMED), which remove the corrupt data*/
    val dropmalformeddf = spark.read.format("csv")
                               .option("header", "true")
                               .option("mode", "DROPMALFORMED")
                               .load("src/inputs/input.csv")
    dropmalformeddf.show()

    /*Reading Data with read mode(FAILFAST), which throw exception
     if any corrupt data present*/
    val failfastdf = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .load("src/inputs/input.csv")
    failfastdf.show()
  }
}
