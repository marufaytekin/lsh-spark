package com.lendap.recommendation

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

/**
 * Created by maruf on 20/08/15.
 */
trait Prediction {
  def predict (ratings: RDD [Rating], kNNList: RDD[(Int, Double)], itemIdd: Int): Double
}
