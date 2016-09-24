package com.puroguramingu.ml

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}



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