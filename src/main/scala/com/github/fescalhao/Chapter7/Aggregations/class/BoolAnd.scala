package com.github.fescalhao.Chapter7.Aggregations.`class`

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

class BoolAnd extends Aggregator[Boolean, Boolean, Boolean]{
  override def zero: Boolean = true

  override def reduce(b: Boolean, a: Boolean): Boolean = b == a

  override def merge(b1: Boolean, b2: Boolean): Boolean = b1 == b2

  override def finish(reduction: Boolean): Boolean = reduction

  override def bufferEncoder: Encoder[Boolean] = Encoders.scalaBoolean

  override def outputEncoder: Encoder[Boolean] = Encoders.scalaBoolean
}
