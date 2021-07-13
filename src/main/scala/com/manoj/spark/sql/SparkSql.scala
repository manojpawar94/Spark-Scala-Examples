package com.manoj.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object SparkSql {

  def main(args: Array[String]): Unit = {
    val path = "/Users/manojpawar/eclipse-workspace/test-data"
    val accessCodeFile = path + "/access-code.csv"
    val pwdRecoveryCodFile = path + "/access-code-password-recovery-code.csv"

    val spark = SparkSession.builder().appName("spark-sql-app").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val accessCodeDF = createAndShowDF(spark, accessCodeFile)
    val pwdRecoveryCodeDF = createAndShowDF(spark, pwdRecoveryCodFile)

    accessCodeDF.join(pwdRecoveryCodeDF, "First_Name").show
    accessCodeDF.join(pwdRecoveryCodeDF, accessCodeDF("First_Name") === pwdRecoveryCodeDF("First_Name"), "left_outer").show

    accessCodeDF.createOrReplaceTempView("Access_Code")
    pwdRecoveryCodeDF.createTempView("Pwd_Recovery_Code")

    spark.sql("select * from Access_Code A, Pwd_Recovery_Code P where A.First_Name = P.First_Name").show

    spark.close
  }

  def createAndShowDF(spark: SparkSession, path: String): DataFrame = {
    val dataFrame = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path)
    dataFrame.show
    dataFrame.printSchema
    println("No of record in dataframe:: " + dataFrame.count)
    return dataFrame
  }
}