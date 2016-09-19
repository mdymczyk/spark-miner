package com.puroguramingu

import org.scalatest.FunSuite

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
    ListBuffer("mixed", "types")
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

    val expectedDeg = Map("compatibility" -> 1, "construction" -> 1, "Criteria" -> 1, "used" -> 1,
      "These" -> 2, "set" -> 6, "strict" -> 2, "numbers" -> 2, "constraints" -> 2,
      "corresponding" -> 2, "mixed" -> 2, "components" -> 1, "supporting" -> 3, "Compatibility"
        -> 1, "solving" -> 1, "Upper" -> 2, "sets" -> 3, "considered" -> 3, "bounds" -> 2,
      "solutions" -> 3, "nonstrict" -> 2, "inequations" -> 4, "criteria" -> 2, "constructing" ->
        1, "algorithms" -> 3, "Diophantine" -> 3, "types" -> 5, "systems" -> 4, "linear" -> 5,
      "generating" -> 3, "equations" -> 3, "minimal" -> 8, "natural" -> 2, "given" -> 1)

    val expectedCooc = Map("compatibility" -> Map("compatibility" -> 2), "construction" -> Map
    ("construction" -> 2), "Criteria" -> Map("Criteria" -> 2), "used" -> Map("used" -> 2),
      "These" -> Map("These" -> 2, "criteria" -> 1), "set" -> Map("set" -> 6, "supporting" -> 1,
        "minimal" -> 2), "strict" -> Map("strict" -> 2, "inequations" -> 1), "numbers" -> Map
      ("numbers" -> 2, "natural" -> 1), "constraints" -> Map("constraints" -> 2, "linear" -> 1),
      "corresponding" -> Map("corresponding" -> 2, "algorithms" -> 1), "mixed" -> Map("mixed" ->
        2, "types" -> 1), "components" -> Map("components" -> 2), "supporting" -> Map("set" -> 1,
        "supporting" -> 2, "minimal" -> 1), "Compatibility" -> Map("Compatibility" -> 2),
      "solving" -> Map("solving" -> 2), "Upper" -> Map("Upper" -> 2, "bounds" -> 1), "sets" ->
        Map("sets" -> 2, "generating" -> 1, "minimal" -> 1), "considered" -> Map("considered" ->
        4, "types" -> 1), "bounds" -> Map("Upper" -> 1, "bounds" -> 2), "solutions" -> Map
      ("solutions" -> 6), "nonstrict" -> Map("nonstrict" -> 2, "inequations" -> 1), "inequations"
        -> Map("strict" -> 1, "nonstrict" -> 1, "inequations" -> 4), "criteria" -> Map("These" ->
        1, "criteria" -> 2), "constructing" -> Map("constructing" -> 2), "algorithms" -> Map
      ("corresponding" -> 1, "algorithms" -> 4), "Diophantine" -> Map("Diophantine" -> 2,
        "equations" -> 1, "linear" -> 1), "types" -> Map("mixed" -> 1, "considered" -> 1, "types"
        -> 6), "systems" -> Map("systems" -> 8), "linear" -> Map("constraints" -> 1,
        "Diophantine" -> 1, "equations" -> 1, "linear" -> 4), "generating" -> Map("sets" -> 1,
        "generating" -> 2, "minimal" -> 1), "equations" -> Map("Diophantine" -> 1, "equations" ->
        2, "linear" -> 1), "minimal" -> Map("set" -> 2, "supporting" -> 1, "sets" -> 1,
        "generating" -> 1, "minimal" -> 6), "natural" -> Map("numbers" -> 1, "natural" -> 2),
      "given" -> Map("given" -> 2))

    assertResult(expectedDeg)(deg)
    assertResult(expectedCooc)(cooc)

    bookDoc.replaceAll("[.,\n]", " ")
      .split("[ \t]")
      .filter(word => !stopwords.contains(word) && word.nonEmpty)
      .groupBy(w => w)
      .map(t => (t._1, t._2.length))
      .foreach { t =>
        assert(freq.get(t._1).get == t._2)
      }

  }

}
