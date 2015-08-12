package com.lendap.lsh

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg.{Vectors, SparseVector}
import org.apache.spark.rdd.RDD


/**
 * Created by maytekin on 05.08.2015.
 */
object Main {

  def main(args: Array[String]) {
    val dataFile = "data/ml-1m.data"
    val conf = new SparkConf().setAppName("LSH").setMaster("local")
    val sc = new SparkContext(conf)
    //read data file in as a RDD, partition RDD across <partitions> cores
    val data = sc.textFile(dataFile)
    val usersRDD = data.map(line => line.split("\t")).map(elems => elems(0).toLong).distinct();
    val itemsRDD = data.map(line => line.split("\t")).map(elems => elems(1).toLong).distinct();
    val userItemMatrixRDD = data
      .map(line => line.split("\t"))
      .map(elems => (elems(0).toLong, (elems(1).toInt, elems(2).toDouble)))
    val userRatingsRDD = userItemMatrixRDD.groupByKey()
    val userRatingsFormedRDD = userRatingsRDD
      .map(users => (users._1, Vectors.sparse(users._2.toList.size, users._2.toSeq).asInstanceOf[SparseVector]))
    println(itemsRDD.takeOrdered(10).toList)
    println(userRatingsFormedRDD.take(1))
    //val users_gt50 = userRatingsRDD.map(a=>(a._1, a._2.size)).filter(_._2 > 50)
    //println(userRatingsRDD.take(1).map(a=>(a._1, a._2.size)).toList)

    //val m = 100 /** number of elements */
    //val h = Hasher.create(itemsRDD.count().toInt)
    //val i = Array(0, 2, 3, 6)
    //val v = Array(1.0, -4.0, 6.0, -20.0)
    //print(h.hash(new SparseVector(4, i, v)))
    val numHashFunc = 8
    //val m = new LSH(data =

  }

}
