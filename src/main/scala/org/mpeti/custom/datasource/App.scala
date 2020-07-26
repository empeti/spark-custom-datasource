package org.mpeti.custom.datasource

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    println("Application started...")

    val conf = new SparkConf().setAppName("spark-custom-source")
    val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()

    val df = spark.sqlContext.read.format("org.mpeti.custom.datasource").load("data/")
    //df.printSchema()
    df.show()

    println("Application started...")
  }
}
