package com.jemson.scala.spark

import org.apache.spark.sql.SparkSession

import scala.reflect.io.Directory

object TestCache {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("TestCache")
      .getOrCreate()

    val projectDir = System.getProperty("user.dir")  //项目目录
    println(projectDir)

    val logFile = projectDir + "/files/persistData.txt"


   val lines = spark.read.textFile(logFile)
   import spark.implicits._
   val works = lines.flatMap(line => line.split("\t"))
   val ones =  works.map(w => (w,1))
    val onesRDD = ones.rdd
    val counts = onesRDD.reduceByKey((v1,v2) => v1 + v2)
    counts.foreach(println)

    spark.stop()
  }
}
