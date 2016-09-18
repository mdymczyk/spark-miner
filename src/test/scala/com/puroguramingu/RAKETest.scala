package com.puroguramingu

import org.scalatest.FunSuite

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class RAKETest extends FunSuite {

  val stopwords = Set("a", "about", "above", "above", "across", "after", "afterwards", "again",
    "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am",
    "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone",
    "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became",
    "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind",
    "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but",
    "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de",
    "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either",
    "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone",
    "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire",
    "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front",
    "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her",
    "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself",
    "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into",
    "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd",
    "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover",
    "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither",
    "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not",
    "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or",
    "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part",
    "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed",
    "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since",
    "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime",
    "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the",
    "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby",
    "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this",
    "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too",
    "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon",
    "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence",
    "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever",
    "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why",
    "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself",
    "yourselves", "the")

  val bookDoc = "Compatibility of systems of linear constraints over the set of natural " +
    "numbers\n\n" +
    "Criteria of compatibility of a system of linear Diophantine equations, strict " +
    "inequations, and nonstrict inequations are considered. Upper bounds for components of a " +
    "minimal set of solutions and algorithms of construction of minimal generating sets of " +
    "solutions for all types of systems are given. These criteria and the corresponding " +
    "algorithms for constructing a minimal supporting set of solutions can be used in solving " +
    "all the considered types of systems and systems of mixed types"

  val bookDocKeywords = Array(ListBuffer("Compatibility"),
    ListBuffer("systems"),
    ListBuffer("linear", "constraints"),
    ListBuffer("set"),
    ListBuffer("natural", "numbers"),
    ListBuffer("Criteria"),
    ListBuffer("compatibility"),
    ListBuffer("linear", "Diophantine", "equations"),
    ListBuffer("strict", "inequations"),
    ListBuffer("nonstrict", "inequations"),
    ListBuffer("considered"),
    ListBuffer("Upper", "bounds"),
    ListBuffer("components"),
    ListBuffer("minimal", "set"),
    ListBuffer("solutions"),
    ListBuffer("algorithms"),
    ListBuffer("construction"),
    ListBuffer("minimal", "generating", "sets"),
    ListBuffer("solutions"),
    ListBuffer("types"),
    ListBuffer("systems"),
    ListBuffer("given"),
    ListBuffer("These", "criteria"),
    ListBuffer("corresponding", "algorithms"),
    ListBuffer("constructing"),
    ListBuffer("minimal", "supporting", "set"),
    ListBuffer("solutions"),
    ListBuffer("used"),
    ListBuffer("solving"),
    ListBuffer("considered", "types"),
    ListBuffer("systems"),
    ListBuffer("systems"),
    ListBuffer("mixed")
  )

  test("Should properly handle empty strings") {
    val rake = new RAKE(stopwords, Array(' '), Array('.', ',', '\n'))
    val res = rake.toRAKESeq("")
    assert(res.isEmpty)
  }

  test("Should parse simple document") {
    val rake = new RAKE(stopwords, Array(' ', '\t'), Array('.', ',', '\n'))
    val res = rake.toRAKESeq(bookDoc)

    assert(res.nonEmpty)
    assertResult(33)(res.length)
    assertResult(bookDocKeywords)(res)
  }

  test("Word statistics") {
    val rake = new RAKE(stopwords, Array(' ', '\t'), Array('.', ',', '\n'))
    val (cooc, deg, freq) = rake.wordStats(rake.toRAKESeq(bookDoc))

  }

}
