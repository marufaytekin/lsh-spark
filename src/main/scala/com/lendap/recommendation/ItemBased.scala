package com.lendap.recommendation

import org.apache.spark.Logging
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

/**
 * Created by maruf on 20/08/15.
 */

object ItemBased extends Serializable with Prediction with Logging {

  /** Given a target user id and nearest neighbors of a target item, item-based rating prediction
    * algorithm is used to predict rating of the itemId for the target user.
    *
    * @param ratings an RDD of user item ratings
    * @param kNNList k number of nearest neighbors to the target item in (itemId, similarity_weight) format
    * @param userId target user id
    * */
  override def predict(ratings: RDD[Rating], kNNList: RDD[(Int, Double)], userId: Int): Double =
    kNNList
      .map(a => ratings.filter(x => x.user == userId && x.product == a._1)
      .take(1).asInstanceOf[Rating].rating * a._2)
      .reduce(_ + _) / kNNList.map(a => a._2).reduce(_ + _)

}

