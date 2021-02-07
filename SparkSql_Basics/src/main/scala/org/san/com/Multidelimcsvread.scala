package org.san.com

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import javax.swing.DefaultRowSorter.Row

object Multidelimcsvread {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.bin.dir", "C:\\winutils\\")
    /*creating spark session object to submit all the spark related apis*/
    val spark = SparkSession.builder().master("local[*]").appName("sparkbasics").getOrCreate()

  import spark.implicits._

    /*Older method(Apply in Spark 2.x)*/

    val multiinput = spark.read.text("src/inputs/multidelim.csv")
    /*storing header to an array*/
    val header = multiinput.first().mkString("").split("&&")
    /*Creating the schema dynamically*/
    val schema = StructType(header.map(item => StructField(item,StringType)))
    /*filtering out the header and converting to Row Rdd*/
    val multidata = multiinput.filter(d => !d.toString().contains("country_name")).rdd
    /*Transforming the Row Rdd by removing multyi delimitter*/
    val outrow = multidata.map(row =>  Row.fromSeq(row.mkString("").split("&&")))
    /*creating Dataframe with new Row Rdd and schema*/
    val outdf = spark.createDataFrame(outrow, schema)
    outdf.show()
    //End of older method

    /*Multi delimitter Reading in csv in spark3.0(new Approach)*/
    val multidelimnewdf = spark.read.format("csv")
                               .option("header", true)
                               .option("delimitter", "&&")
                               .option("inferschema", true)
                               .csv("src/inputs/multidelim.csv")
    multidelimnewdf.show()
  }
}
