package org.apache.spark.mllib.feature

import org.apache.spark.mllib.linalg.Vector

class GloVeModel private[spark] (private[spark] val wordIndex: Map[String, Long],
                                 private[spark] val wordVectors: Array[Float])
  extends Serializable {
  // TODO with Saveable {

  private val numWords = wordIndex.size
  private val dim = wordVectors.length / numWords

  def transform(word: String): Vector = ???

  def findSynonyms(word: String, num: Int): Array[(String, Double)] = ???

  def findSynonyms(vector: Vector, num: Int): Array[(String, Double)] = ???

}