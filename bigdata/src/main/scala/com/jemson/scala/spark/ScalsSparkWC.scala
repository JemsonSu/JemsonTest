package com.jemson.scala.spark

import org.apache.spark.sql.SparkSession

object ScalsSparkWC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleApp")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val lines = sc.textFile("./bigdata/a.txt")
    val works = lines.flatMap(line => line.split(" "))
    val ones = works.map(work => (work,1))

    val counts = ones.reduceByKey((v1,v2) => v1+v2)

    counts.foreach(t => println(t._1 + "==" + t._2))

  }

}
