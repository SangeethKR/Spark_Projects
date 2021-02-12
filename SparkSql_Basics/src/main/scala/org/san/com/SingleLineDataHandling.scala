package org.san.com

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}
import scala.collection.mutable.ListBuffer

/*case class for the required schema*/
case class schema(col1:String,col2:String,col3:String)

object SingleLineDataHandling {

  def main(args: Array[String]): Unit = {

    /*creating spark seesion object*/
    val spark = UtilFunction.sparkSessionObjectCreate("SingleLineDataHandling")
    import spark.implicits._
    /*single line input file*/
    val singledata = spark.read.text("src/inputs/singleline.txt")

    /*converting the single line input to multi line based on the logic*/
    val rdddata  = singledata.rdd.map(data =>
      {
        /*creating list to add different row data*/
        var datalist = new ListBuffer[schema]

        /*splitting the data with split character*/
        val array = data.mkString("") .split(",")

        /*parsing through the array, create "schema"
          object and add it to listbuffer*/
        for(i<- 0 until  array.length-1 by 3) {
          datalist.+=(schema(array(i),array(i+1), array(i+2)))
        }
        datalist
      }/*flattening the list in the rdd*/
    ).flatMap(list=> list)

    /*converting it to Dataframe*/
    rdddata.toDF().show()


  }
}
