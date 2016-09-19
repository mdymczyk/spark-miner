package com.puroguramingu

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}

/**
  * Rapid Automatic Keyword Extraction algorithm for extracting
  * keywords from documents as described in "Text Mining: Applications
  * and Theory" by Michael W. Berry and Jacob Kogan
  *
  * @param stopwords    Set of words which will not be taken into account
  * @param wordDelims   Array of characters used to break down the documents into tokens
  * @param phraseDelims Array of characters used to break down the document into phrases
  * @param uid
  */
final class RAKE(val stopwords: Set[String],
                 val wordDelims: Array[Char],
                 val phraseDelims: Array[Char],
                 override val uid: String) extends Estimator[RAKEModel] {

  def this(stopwords: Set[String],
           wordDelims: Array[Char],
           phraseDelims: Array[Char]) =
    this(stopwords, wordDelims, phraseDelims, Identifiable.randomUID("rake"))

  override def fit(dataset: Dataset[_]): RAKEModel = ???

  /**
    * Transforms an input document into a RAKE sequence of keywords,
    * where each keyword is a sequence of one or more tokens.
    *
    * This method run in linear time as a state machine checking
    * the whole document character by character.
    *
    * @param doc The document to be sequencialized
    * @return
    */
  def toRAKESeq(doc: String): Seq[Seq[String]] = {
    val sequences = mutable.ListBuffer[Seq[String]]()
    var sequence = mutable.ListBuffer[String]()

    var currWord = new StringBuilder()

    doc.foreach { currChar =>
      if (wordDelims.contains(currChar)) {
        if (!stopwords.contains(currWord.toString())) {
          if (currWord.nonEmpty) sequence += currWord.toString()
        } else {
          if (sequence.nonEmpty) sequences += sequence
          sequence = mutable.ListBuffer[String]()
        }
        currWord = new StringBuilder()
      } else if (phraseDelims.contains(currChar)) {
        if (!stopwords.contains(currWord.toString()) && currWord.nonEmpty) {
          sequence += currWord.toString()
        }
        currWord = new StringBuilder()
        if (sequence.nonEmpty) sequences += sequence
        sequence = mutable.ListBuffer[String]()
      } else {
        currWord += currChar
      }
    }

    if (currWord.nonEmpty) sequence += currWord.toString()

    if (sequence.nonEmpty) sequences += sequence

    sequences
  }

  /**
    * Method returning the word co-occurrence matrix of all the words in the corpus,
    * the frequency of each word (how many times it appeared in the whole corpus)
    * and the degree of each word (what is the total size of all the phrases in which
    * a given word occurred).
    *
    * @param corpus The corpus to be examined, which consists of a sequence of phrases (sequences
    *               of [[String]])
    * @return
    */
  def wordStats(corpus: Seq[Seq[String]]):
  (MMap[String, MMap[String, Long]], MMap[String, Long], MMap[String, Long]) = {
    val coocMat = MMap[String, MMap[String, Long]]()
    val deg = MMap[String, Long]()
    val freq = MMap[String, Long]()
    // TODO threadsafety?!
    corpus.foreach { doc =>
      for (i <- doc.indices) {
        freq.put(doc(i), freq.getOrElse[Long](doc(i), 0) + 1)
        deg.put(doc(i), deg.getOrElse[Long](doc(i), 0) + doc.length)
        for (j <- i until doc.length) {
          var row: MMap[String, Long] =
            coocMat.getOrElse(doc(i), MMap[String, Long]())
          row.put(doc(j), row.getOrElse[Long](doc(j), 0) + 1)
          coocMat.put(doc(i), row)

          row = coocMat.getOrElse(doc(j), MMap[String, Long]())
          row.put(doc(i), row.getOrElse[Long](doc(i), 0) + 1)
          coocMat.put(doc(j), row)
        }
      }
    }
    (coocMat, deg, freq)
  }

  def fit(dataset: RDD[String]): RAKEModel = {
    dataset.foreach(toRAKESeq(_))
    null
  }

  override def copy(extra: ParamMap): Estimator[RAKEModel] = ???

  override def transformSchema(schema: StructType): StructType = ???
}

class RAKEModel(override val uid: String) extends Model[RAKEModel] {

  def this() = this(Identifiable.randomUID("rakeModel"))

  override def copy(extra: ParamMap): Nothing = ???

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def transformSchema(schema: StructType): StructType = ???

}
