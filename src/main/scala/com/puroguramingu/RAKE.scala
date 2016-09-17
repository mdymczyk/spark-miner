package com.puroguramingu

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

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

    if (sequence.nonEmpty) sequences += sequence

    sequences
  }

  def fit(dataset: RDD[String]): RAKEModel = {
    dataset.foreach(toRAKESeq(_))
    null
  }

  override def copy(extra: ParamMap): Estimator[RAKEModel] = ???

  override def transformSchema(schema: StructType): StructType = ???
}

class RAKEModel extends Model[RAKEModel] {

  override def copy(extra: ParamMap): Nothing = ???

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def transformSchema(schema: StructType): StructType = ???

  override val uid: String = ""
}
