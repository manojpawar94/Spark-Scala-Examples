package com.manoj.spark.compare

import org.apache.spark.sql.SparkSession


object Compare {

  val path = "/Users/manojpawar/eclipse-workspace/test-data"
  val accessCodeFile = path + "/access-code.csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-sql-app")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //reading the source records
    val sourceDF  = spark.read
      .option("delimiter", ";")
      .option("header", "true")
      .csv(accessCodeFile)

    sourceDF.show

    val targetDF = spark.read
      .option("delimiter", ";")
      .option("header", "true")
      .csv(accessCodeFile)

    targetDF.show

    val resultantDF = sourceDF.join(targetDF, sourceDF("First_Name") === targetDF("First_Name"))
    resultantDF.show

  }
  
 
}