package com.lendap.lsh

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg.{Vectors, SparseVector}


/**
 * Created by maytekin on 05.08.2015.
 */
object Main {

  /** Sample usage of LSH movie rating data.*/
  def main(args: Array[String]) {

    //init spark context
    val numPartitions = 8
    val dataFile = "data/ml-1m.data"
    val conf = new SparkConf()
      .setAppName("LSH")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    //read data file in as a RDD, partition RDD across <partitions> cores
    val data = sc.textFile(dataFile, numPartitions)

    //parse data and create (user, item, rating) tuples
    val ratingsRDD = data
      .map(line => line.split("::"))
      .map(elems => (elems(0).toInt, elems(1).toInt, elems(2).toDouble))

    //list of distinct items
    val items = ratingsRDD.map(x => x._2).distinct()
    val maxIndex = items.max + 1

    //user ratings grouped by user_id
    val userItemRatings = ratingsRDD.map(x => (x._1, (x._2, x._3))).groupByKey().cache()

    //convert each user's rating to tuple of (user_id, SparseVector_of_ratings)
    val sparseVectorData = userItemRatings
      .map(a=>(a._1.toLong, Vectors.sparse(maxIndex, a._2.toSeq).asInstanceOf[SparseVector]))

    //run locality sensitive hashing model with 6 bands and 8 hash functions
    val lsh = new LSH(sparseVectorData, maxIndex, numHashFunc = 8, numBands = 6)
    val model = lsh.run

    //print sample hashed vectors in ((bandId#, hashValue), vectorId) format
    model.bands.take(10) foreach println

    //get the near neighbors of userId: 4587 in the model
    val candList = model.getCandidates(4587)
    println("Number of Candidate Neighbors: ")
    println(candList.count())
    println("Candidate List: " + candList.collect().toList)

    //save model
    val temp = "target/" + System.currentTimeMillis().toString
    model.save(sc, temp)

    //load model
    val modelLoaded = LSHModel.load(sc, temp)

    //print out 10 entries from loaded model
    modelLoaded.bands.take(15) foreach println


  }

}
