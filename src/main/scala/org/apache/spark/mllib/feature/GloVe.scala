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
  private var window = 5

  def setWindow(window: Int): this.type = {
    require(window > 0,
      s"window must be positive but got $window")
    this.window = window
    this
  }

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

//  private var vHash: RDD[(String, Long)] = _

  // Co-occurrence matrix
  private val cm: Map[(Int, Int), Double] = null

  private def cooccurrence(corpus: RDD[_ <: Iterable[String]]): Map[(Int, Int), Double] = {
    val vocab: RDD[(String, Int)] = corpus
      .flatMap(x => x)
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > minCount)

    val wHash = vocab
      .zipWithIndex()
      .map{ case((word, cnt), idx) => (word, idx)}
      .collectAsMap()

    val wHashBC = corpus.sparkContext.broadcast(wHash)

    val coocurences = scala.collection.mutable.HashMap.empty[(Long, Long), Double]
    vocab.keys.mapPartitions{ it => {
      val buffer = new CircularQueue[Long](limit = window)

      it.foreach{ word =>
        wHashBC.value.get(word).foreach{ wh =>
          buffer.filter(_ != wh).foreach { bwh =>
            val wh1 = Math.min(wh, bwh)
            val wh2 = Math.max(wh, bwh)
            coocurences.put((wh1, wh2), coocurences.getOrElse((wh1, wh2), 0))
          }
        }

      }

      null
    }}

    null
  }

  class CircularQueue[A](val limit: Int = 5, list: Seq[A] = Seq()) extends Iterator[A]{

    val elements = new mutable.Queue[A] ++= list
    var pos = 0

    // TODO Not thread safe
    def next: A = {
      if (pos >= elements.length) {
        pos = 0
      }
      val value = elements(pos)
      pos = pos + 1
      value
    }

    def hasNext: Boolean = elements.nonEmpty

    // TODO Not thread safe
    def add(a: A): Unit = {
      if (elements.size == limit) {
        elements.dequeue()
      }
      elements += a
    }

    override def toString = elements.toString

  }

  def fit(input: RDD[Seq[String]]): GloVeModel = {
    null
  }
}