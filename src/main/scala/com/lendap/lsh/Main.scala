package com.lendap.lsh

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vectors, SparseVector}
import java.io._


/**
 * Created by maytekin on 05.08.2015.
 */
object Main {

  def main(args: Array[String]) {
    val dataFile = "data/amazon-books-1m.data"
    val conf = new SparkConf().setAppName("LSH").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile(dataFile).cache()
    /*val pairs = data.map(line => line.split('\t'))
      .map(l => (l(0), 1))
      .reduceByKey(_ + _)
      .filter{case (k, v) => v >= 20}
      .map(x=> (x._2,x._1))
    pairs.collect.foreach(println)
    println(pairs.count)*/

    val h = Hasher.create(-5, 5, 7)
    val i = Array(0, 2, 3, 6)
    val v = Array(1.0, -4.0, 6.0, 20.0)
    print(h.hash(new SparseVector(4, i, v)))

  }

}
