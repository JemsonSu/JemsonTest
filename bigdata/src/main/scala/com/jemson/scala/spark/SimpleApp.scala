package com.jemson.scala.spark

import org.apache.spark.{SparkConf, SparkContext}


object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile =  "./bigdata/a.txt"

    val conf = new SparkConf()
    conf.setAppName("SimpleApp")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")
    val lines = sc.textFile(logFile)
    val workds = lines.flatMap(line => line.split(" "))
    val ones = workds.map(work => (work,1))
    val counts = ones.reduceByKey((v1,v2) => v1+v2)
    counts.foreach(t => println(t._1 + "==" + t._2))




  }
}
