package org.apache.spark.mllib.feature

import breeze.linalg.Matrix
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.SparseMatrix
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.collection.{Map, mutable}

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

  /**
    * TRAINING
    */
  import breeze.linalg._
  def fit(input: RDD[Seq[String]]): GloVeModel = {
    // TODO memory killer!!!
    val (cm, wHashBC) = cooccurrenceMatrix(input)

    cm.mapPartitions { entries =>
      val WUpdates1 = Array[Double](dim)
      val WUpdates2 = Array[Double](dim)

      // TODO make this a sparse matrix
      val W: Matrix[Double] = DenseMatrix.zeros[Double]()

      // TODO threadsafety?
      // TODO change this to a reduce function
      entries.foreach{ case ((w1, w2), cooc) =>
        var vec = W(w1.toInt, ::)
        var diff = W(w1, ::)*W(w2, ::)
      }

      null
    }

    null
  }

  /**
    * Calculates the co-occurrence matrix from a given corpus using a sliding, symmetrical window.
    *
    * @param corpus Corpus used to calculate co-occurrences. Each Iterable is considered as a
    *               single sentence.
    * @return
    */
  // TODO should the model also save the word -> index mapping?
  private[feature] def cooccurrenceMatrix(corpus: RDD[_ <: Iterable[String]])
  : (RDD[((Long, Long), Double)], Broadcast[Map[String, Long]]) = {
    val wHash = corpus
      .flatMap(x => x)
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
      .keys
      .zipWithIndex()
      .collectAsMap()

    val wHashBC = corpus.sparkContext.broadcast(wHash)

    (corpus.mapPartitions { it => {
      val coocurences = scala.collection.mutable.HashMap.empty[(Long, Long), Double]
      val buffer = new CircularQueue[Long](limit = window)

      it.foreach { words =>
        words.foreach { word =>
          wHashBC.value.get(word).foreach { wh =>
            buffer.foreach { bwh =>
              if (bwh != wh) {
                val wh1 = Math.min(wh, bwh)
                val wh2 = Math.max(wh, bwh)
                // TODO apply here different strategies for weights
                coocurences.put((wh1, wh2), coocurences.getOrElse[Double]((wh1, wh2), 0) + 1)
              }
            }
            buffer.add(wh)
            buffer.reset()
          }
        }
      }
      coocurences.iterator
    }
    }.reduceByKey(_ + _), wHashBC)
  }

  class CircularQueue[A](val limit: Int = 5, list: Seq[A] = Seq()) extends Iterator[A] {

    val elements = new mutable.Queue[A] ++= list
    var pos = 0

    // TODO Not thread safe
    def next: A = {
      val value = elements(pos)
      pos = pos + 1
      value
    }

    def reset(): Unit = pos = 0

    def hasNext: Boolean = pos < elements.size

    // TODO Not thread safe
    def add(a: A): Unit = {
      if (elements.size == limit) {
        elements.dequeue()
      }
      elements += a
    }

    override def toString: String = elements.toString

  }

}