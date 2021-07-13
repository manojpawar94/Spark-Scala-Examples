package com.manoj.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkConf

object WordCount {

  def main(args: Array[String]): Unit = {
    val path = "/Users/manojpawar/eclipse-workspace/test-data/test-file.txt"
    val config = new SparkConf().setAppName("Spark-Demo-App").setMaster("local[*]")
    val sc = new SparkContext(config)
    textFile(sc, path)
  }

  def textFile(sc: SparkContext, path: String) {
    val fileRdd = sc.textFile(path).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    fileRdd.collect().foreach(println)
  }

}