package org.san.com
import org.apache.spark.sql.functions._

object ExplodeFunction {

  def main(args: Array[String]): Unit = {

    val spark = UtilFunction.sparkSessionObjectCreate("ExplodeFunction")

    import spark.implicits._

    /*Input file reading*/
    val indf = spark.read.option("delimiter", "|")
                         .option("inferschema", true)
                         .option("header", true)
                         .csv("src/inputs/explodeinput1.csv")


    /*normal explode function(empty and null values are not considered)*/
    val normal_explode_df = indf.withColumn("courses", explode(split($"courses", ",")))
    normal_explode_df.show()

    /*explode_outer(empty and null values also are considered.)*/
    val explode_outer_df = indf.withColumn("courses", explode_outer(split($"courses", ",")))
    explode_outer_df.show()


    /*posexplode function(give index to the values in the array)*/
    val posexplode_df = indf.select( posexplode(split($"courses", ",")))
    posexplode_df.show()

  }
}
