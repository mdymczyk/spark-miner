package org.apache.spark.ml.feature

import org.apache.spark.ml.Model
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.feature
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class GloVeModel private[ml](override val uid: String,
                             @transient private val wordVectors: feature.GloVeModel)
  extends Model[GloVeModel] with GloVeBase {
  // TODO with MLWritable {

  @transient lazy val getVectors: DataFrame = ???

  def findSynonyms(word: String, num: Int): DataFrame = ???

  def findSynonyms(word: Vector, num: Int): DataFrame = ???

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): GloVeModel = {
    val copied = new GloVeModel(uid, wordVectors)
    copyValues(copied, extra).setParent(parent)
  }

}