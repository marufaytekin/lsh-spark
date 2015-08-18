package com.lendap.recommendation

import org.apache.spark.Logging
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

/**
 * Created by maruf on 20/08/15.
 */
object UBPrediction extends Serializable with Prediction with Logging {

  /** Given a target item id and nearest neighbors list user-based rating prediction algorithm
    * is used to predict rating of itemId for a target user.
    *
    * @param ratings an RDD of user item ratings
    * @param kNNList k number of nearest neighbors to target user in (userId, similarity_weight) format
    * @param itemId target item number
    * */
  override def predict(ratings: RDD[Rating], kNNList: RDD[(Int, Double)], itemId: Int): Double =
    kNNList
      .map(a => ratings.filter(x => x.user == a._1 && x.product == itemId)
      .take(1).asInstanceOf[Rating].rating * a._2)
      .reduce(_ + _) / kNNList.map(a => a._2).reduce(_ + _)
}

