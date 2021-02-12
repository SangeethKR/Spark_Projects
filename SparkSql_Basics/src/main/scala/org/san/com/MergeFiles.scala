package org.san.com

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object MergeFiles {

  def main(args: Array[String]): Unit = {

    /*creating spark sessionn object*/
    val spark = UtilFunction.sparkSessionObjectCreate("MergeFiles")
    import spark.implicits._
    /*Reading input1*/
    val input1 = spark.read
                      .option("delimiter", ",")
                      .option("header", "true")
                      .csv("src/inputs/mergefile1.csv")

    /*Reading input2*/
    val input2 = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .csv("src/inputs/mergefile2.csv")

    /*Method1 for merging files*/
    /*Getting schema of 2 input files*/
    val fschema = input1.columns.toSet
    val sschema = input2.columns.toSet
    /*creating a combine schema*/
    val tschema =fschema ++ sschema

    /*Function for creating the select expression*/
    def selectexpr(scolumns:Set[String], tcolumns:Set[String])={
      tcolumns.toList.map(column =>
        if (scolumns.contains(column)) {
          col(column)
        }
        else {
          lit(null).as(column)
      }

      )
    }
    /*creating merged Dataframe,calling function to create select expression*/
    val mergeddf = input1.select(selectexpr(fschema,tschema):_*).union(input2.select(selectexpr(sschema,tschema):_*))

    mergeddf.show()
    /*End of Method1*/




    /*Method2 for merging files*/
    /*Finding common columns for using tyhem in join condition*/
    val joincols = input1.columns.toSet.intersect(input2.columns.toSet).toList
    /*Doing outer Join 2 Dataframes on common columns */
    val mergedDf = input1.join(input2, joincols,"outer")

    mergedDf.show()
    /*End of Method2*/

  }

}
