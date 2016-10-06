package miner.keywords

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

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
                 override val uid: String) extends Transformer
  with RAKEParams
  with DefaultParamsWritable {

  def this(stopwords: Set[String],
           wordDelims: Array[Char] = Array[Char](' ', '\t'),
           phraseDelims: Array[Char] = Array[Char](',', '.')) =
    this(stopwords, wordDelims, phraseDelims, Identifiable.randomUID("rake"))

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def copy(extra: ParamMap): Transformer = ???

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[StringType],
      s"The input column must be StrignType, but got $inputType.")
    val outc = $(outputCol)
    StructType(schema.fields ++ Seq(StructField(outc,
      MapType(ArrayType(StringType), DoubleType),
      nullable = true)))
  }

  /**
    * Extracts keywords according to the RAKE algorithm and scores them according to
    * the [[RAKEStrategy]]. The resulting mapping returns unique, lowercased,
    * keywords, contrary to the [[RAKE.toRAKESeq(doc)]] method, summing up all the scores for a
    * given keyword.
    *
    * @param doc      Document to be scored
    * @param strategy Strategy used for calculating the score
    * @return A map of keywords and their scores
    */
  // TODO implement adjoining keywords as per chapter 1.2.3 of the book
  def toScoredKeywords(doc: String,
                       strategy: RAKEStrategy = RAKEStrategy.Deg): Map[Seq[String], Double] = {
    val raked = toRAKESeq(doc).map(_.map(_.toLowerCase))
    val (cooc, deg, freq) = wordStats(raked)
    raked.map { phrase =>
      val score = strategy match {
        case RAKEStrategy.Deg =>
          phrase.foldLeft(0.0) { case (acc, token) =>
            acc + deg.get(token).get
          }
        case RAKEStrategy.Freq =>
          phrase.foldLeft(0.0) { case (acc, token) =>
            acc + freq.get(token).get
          }
        case RAKEStrategy.Ratio =>
          phrase.foldLeft(0.0) { case (acc, token) =>
            acc + deg.get(token).get.toDouble / freq.get(token).get.toDouble
          }
      }
      (phrase, score)
    }.groupBy(_._1).map { case (k, vs) => (k, vs.map(_._2).sum) }
  }

  /**
    * Transforms an input document into a RAKE sequence of keywords,
    * where each keyword is a sequence of one or more tokens.
    *
    * This method runs in linear time (proportional to the number of
    * characters in the document) as a state machine checking
    * the whole document character by character.
    *
    * Example:
    *
    * Document: I ate cake, pie and a blueberry scone and then strawberry pie.
    * Stopwords: ["I", "a", "and", "then"]
    * Word delimeters: [' ']
    * Phrase delimeters: ['.', ',']
    *
    * Would result in a following sequence of sequences:
    *
    * [ ["ate", "cake"], ["pie"], ["blueberry", "scone"], ["strawberry", "pie"] ]
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
    * Example:
    *
    * Document: I ate cake, pie and a blueberry scone and then strawberry pie.
    * Stopwords: ["I", "a", "and", "then"]
    * Word delimeters: [' ']
    * Phrase delimeters: ['.', ',']
    *
    * Would result in the following 3 structures:
    * [ ["ate", "cake"], ["pie"], ["blueberry", "scone"], ["strawberry", "pie"] ]
    *
    * Cooccurrence matrix:
    * [
    * "ate" -> ["ate" -> 1, "cake" -> 1],
    * "cake" -> ["cake" -> 1, "ate" -> 1],
    * "pie" -> ["pie" -> 2, "strawberry" -> 1],
    * "blueberry" -> ["blueberry" -> 1, "scone" -> 1],
    * "scone" -> ["scone" -> 1, "blueberry" -> 1],
    * "strawberry" -> ["strawberry" -> 1, "pie" -> 1]
    * ]
    *
    * Frequency:
    * ["ate" -> 1, "cake" -> 1, "pie" -> 2, "blueberry" -> 1, "scone" -> 1, "strawberry" -> 1]
    *
    * Degree:
    * ["ate" -> 2, "cake" -> 2, "pie" -> 3, "blueberry" -> 2, "scone" -> 2, "strawberry" -> 1]
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
    // TODO threadsafety?
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

}

trait RAKEParams extends Params {
  val inputCol = new Param[String](this, "inputCol", "input column")
  val outputCol = new Param[String](this, "outputCol", "output column")

  def pvals(pm: ParamMap): (String, String) = ($(inputCol), $(outputCol))
}

/**
  * Strategy used to calculate the RAKE score
  * Deg - degree of the token (cummulative size of all the phrases in which a token appears in)
  * Freq - frequncy of the token (how often it appears in the corpus)
  * Ratio - a degree to frequency ratio
  */
object RAKEStrategy extends Enumeration {
  type RAKEStrategy = Value
  val Deg, Freq, Ratio = Value
}