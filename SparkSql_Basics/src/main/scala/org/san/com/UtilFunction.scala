package org.san.com

import org.apache.spark.sql.SparkSession

object UtilFunction {

  def sparkSessionObjectCreate(appname:String):SparkSession={

    System.setProperty("hadoop.bin.dir", "C:\\winutils\\")
    /*creating spark session object to submit all the spark related apis*/
    val spark = SparkSession.builder()
                            .master("local[*]")
                            .appName(appname)
                            .getOrCreate()
    spark
  }
}
