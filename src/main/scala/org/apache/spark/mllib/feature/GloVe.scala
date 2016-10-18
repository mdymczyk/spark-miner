package org.apache.spark.mllib.feature

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.{Map, mutable}

import com.github.fommil.netlib.BLAS.{getInstance => blas}

class GloVe extends Serializable with Logging {

  // The dimension of resulting word vectors
  private var dim = 50
  // Gradient descent step size
  private var learningRate = 0.05
  private var alpha = 0.75
  // Maximum number of iterations
  private var numIterations = 25
  private var seed = Utils.random.nextLong()
  // The minimum number of times a word has to occur in the corpus to be taken into account
  private var minCount = 5
  // The sliding window size used to compute cooccurrences
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
  def fit(input: RDD[Seq[String]]): GloVeModel = {
    val (cm, wHashBC) = cooccurrenceMatrix(input)

    val initRandom = new XORShiftRandom(seed)

    val sc = input.sparkContext
    val vocabSize: Int = wHashBC.value.size

    val W = Array.fill[Float](vocabSize * dim)((initRandom.nextFloat() - 0.5f) / dim)
    val diff = new Array[Float](vocabSize * dim)
    for (k <- 1 to numIterations) {
      val bcW = sc.broadcast(W)
      val bcDiff = sc.broadcast(diff)

      val partial = cm.mapPartitionsWithIndex { case (idx, entries) =>
        val random = new XORShiftRandom(seed ^ ((idx + 1) << 16) ^ ((-k - 1) << 8))

        val WUpdates = Array[Int](dim)
        val diffUpdates = Array[Int](dim)

        val model = entries.foldLeft((bcW.value, bcDiff.value)) {
          // TODO implement
          case ((foldW, folddiff), entry) => (foldW, folddiff)
        }
        val WLocal = model._1
        val diffLocal = model._2
        // Only output modified vectors.
        Iterator.tabulate(vocabSize) { index =>
          if (WUpdates(index) > 0) {
            Some((index, WLocal.slice(index * dim, (index + 1) * dim)))
          } else {
            None
          }
        }.flatten ++ Iterator.tabulate(vocabSize) { index =>
          if (diffUpdates(index) > 0) {
            Some((index + vocabSize, diffLocal.slice(index * dim, (index + 1) * dim)))

          } else {
            None
          }
        }.flatten
      }
      val synAgg = partial.reduceByKey { case (v1, v2) =>
        blas.saxpy(dim, 1.0f, v2, 1, v1, 1)
        v1
      }.collect()
      var i = 0
      while (i < synAgg.length) {
        val index = synAgg(i)._1
        if (index < vocabSize) {
          Array.copy(synAgg(i)._2, 0, W, index * dim, dim)
        } else {
          Array.copy(synAgg(i)._2, 0, diff, (index - vocabSize) * dim, dim)
        }
        i += 1
      }

      bcW.unpersist(false)
      bcDiff.unpersist(false)
    }

    val words = wHashBC.value.toMap
    wHashBC.unpersist()
    new GloVeModel(words, W)
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

    val wHashBC: Broadcast[Map[String, Long]] = corpus.sparkContext.broadcast(wHash)

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