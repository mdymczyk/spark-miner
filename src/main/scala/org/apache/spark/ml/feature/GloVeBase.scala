package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param.{DoubleParam, IntParam, Params}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

private[feature] trait GloVeBase extends Params
  with HasInputCol with HasOutputCol with HasMaxIter with HasStepSize with HasSeed {

  // ============================ OUTPUT DIMENSIONALITY ============================
  final val dim = new IntParam(
    this, "dim", "the dimension of the words after embedding")

  setDefault(dim -> 100)

  def getDim: Int = $(dim)

  // ================================== MIN COUNT ==================================
  final val minCount = new IntParam(this, "minCount", "the minimum number of times a token must " +
    "appear to be included in the GloVe model's vocabulary")

  setDefault(minCount -> 5)

  def getMinCount: Int = $(minCount)

  // ================================== MIN COUNT ==================================
  final val alpha = new DoubleParam(this, "alpha", "parameter in exponent of weighting function")

  setDefault(alpha -> 0.75)

  def getAlpha: Double = $(alpha)

  // ================================== MIN COUNT ==================================
  final val window = new IntParam(this, "window", "the window used for computing the " +
    "co-occurrence matrix")

  setDefault(window -> 5)

  def getWindow: Double = $(window)

  // ===================================== MISC =====================================
  setDefault(stepSize -> 0.05)

  setDefault(maxIter -> 25)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new ArrayType(StringType, true))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}