package org.apache.spark.mllib.feature

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.collection.mutable

class GloVe extends Serializable with Logging {
  private var dim = 50
  private var learningRate = 0.05
  private var alpha = 0.75
  private var numIterations = 25
  private var seed = Utils.random.nextLong()
  private var minCount = 5

  def setAlpha(alpha: Double): this.type = {
    require(alpha > 0,
      s"alpha must be positive but got $dim")
    this.alpha = alpha
    this
  }

  def setDim(dim: Int): this.type = {
    require(dim > 0,
      s"dimension must be positive but got $dim")
    this.dim = dim
    this
  }

  def setLearningRate(learningRate: Double): this.type = {
    require(learningRate > 0,
      s"Initial learning rate must be positive but got $learningRate")
    this.learningRate = learningRate
    this
  }

  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations >= 0,
      s"Number of iterations must be nonnegative but got $numIterations")
    this.numIterations = numIterations
    this
  }

  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  def setMinCount(minCount: Int): this.type = {
    require(minCount >= 0,
      s"Minimum number of times must be nonnegative but got $minCount")
    this.minCount = minCount
    this
  }

  private val vHash = mutable.Map.empty[String, Int]

  // Co-occurrence matrix
  private val cm: Map[(Int, Int), Double] = null

  private def cooccurrence(corpus: RDD[_ <: Iterable[String]]): Map[(Int, Int), Double] = {
    null
  }

  def fit(input: RDD[Seq[String]]): GloVeModel = {
    null
  }
}