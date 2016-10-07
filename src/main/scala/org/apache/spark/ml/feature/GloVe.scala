package org.apache.spark.ml.feature

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.feature
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

final class GloVe(override val uid: String)
  extends Estimator[GloVeModel] with GloVeBase with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("glove"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setDim(value: Int): this.type = set(dim, value)

  def setAlpha(value: Double): this.type = set(alpha, value)

  def setStepSize(value: Double): this.type = set(stepSize, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setMinCount(value: Int): this.type = set(minCount, value)

  override def fit(dataset: Dataset[_]): GloVeModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).rdd.map(_.getAs[Seq[String]](0))
    val wordVectors = new feature.GloVe()
      .setLearningRate($(stepSize))
      .setMinCount($(minCount))
      .setNumIterations($(maxIter))
      .setSeed($(seed))
      .setDim($(dim))
      .fit(input)
    copyValues(new GloVeModel(uid, wordVectors).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Word2Vec = defaultCopy(extra)
}