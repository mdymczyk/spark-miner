package org.apache.spark.mllib.feature

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class GloVeTest extends FunSuite {

  test("Compute co-occurrence matrix.") {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.parallelize(Array(
      "I like pie.",
      "I enjoy cake.",
      "I like swimming."
    ))

    val glove = new GloVe()
    glove.setMinCount(1)
    val cm = glove.cooccurrence(input.map(_.split("\\s")))

    assert(cm.partitioner.nonEmpty)

    assertResult(
      Set(
        ((0, 1), 1.0),
        ((0, 2), 1.0),
        ((0, 3), 1.0),
        ((0, 4), 1.0),
        ((0, 5), 2.0),
        ((1, 5), 1.0),
        ((2, 4), 1.0),
        ((3, 5), 1.0)
      )
    )(cm.collect().toSet)

  }

}
