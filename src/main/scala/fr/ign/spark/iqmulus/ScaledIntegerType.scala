/*
 * Copyright 2015 IGN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.types

import java.nio.ByteBuffer

trait MergeableType {
  def mergeable(that: DataType): Boolean
  def merge(that: DataType): DataType
}

@SQLUserDefinedType(udt = classOf[OffsetScaledIntegerType])
case class OffsetScaledInteger(n: Int, offset: Double, scale: Double) extends Ordered[OffsetScaledInteger] with Serializable {
  def toDouble: Double = n * scale + offset
  def toFloat: Float = toDouble.toFloat
  def toLong: Long = toDouble.toLong
  def toInt: Int = toDouble.toInt
  def toShort: Int = toDouble.toShort
  def toByte: Int = toDouble.toByte

  override def toString = s"$n*$scale+$offset"
  def isZero: Boolean = if (offset == 0) n == 0 else toDouble == 0

  override def compare(other: OffsetScaledInteger): Int = {
    if (scale == other.scale && offset == other.offset) n.compare(other.n)
    else toDouble.compare(other.toDouble)
  }

  override def equals(other: Any): Boolean = other match {
    case OffsetScaledInteger(i, o, s) =>
      if (s == scale)
        if (o == offset) i == n
        else s * (i - n) == o - offset
      else i * s - n * scale == o - offset
    case _ => false
  }

  def +(that: OffsetScaledInteger): OffsetScaledInteger = {
    require(offset == that.offset && scale == that.scale)
    OffsetScaledInteger(n + that.n, 2 * offset, scale)
  }

  def -(that: OffsetScaledInteger): ScaledInteger = {
    require(offset == that.offset && scale == that.scale)
    ScaledInteger(n - that.n, scale)
  }

  def unary_- : OffsetScaledInteger = {
    OffsetScaledInteger(-n, -offset, scale)
  }

  def abs: OffsetScaledInteger = if (toDouble < 0) this.unary_- else this
}

@SQLUserDefinedType(udt = classOf[ScaledIntegerType])
case class ScaledInteger(n: Int, scale: Double) extends Ordered[ScaledInteger] with Serializable {
  def toDouble: Double = n * scale
  def toFloat: Float = toDouble.toFloat
  def toLong: Long = toDouble.toLong
  def toInt: Int = toDouble.toInt
  def toShort: Int = toDouble.toShort
  def toByte: Int = toDouble.toByte

  override def toString = s"$n*$scale"
  def isZero: Boolean = n == 0

  override def compare(other: ScaledInteger): Int = {
    if (scale == other.scale) n.compare(other.n)
    else toDouble.compare(other.toDouble)
  }

  override def equals(other: Any): Boolean = other match {
    case that: ScaledInteger =>
      if (that.scale == scale) that.n == n else toDouble == that.toDouble
    case _ => false
  }

  def +(that: ScaledInteger): ScaledInteger = {
    require(scale == that.scale)
    ScaledInteger(n + that.n, scale)
  }

  def -(that: ScaledInteger): ScaledInteger = {
    require(scale == that.scale)
    ScaledInteger(n - that.n, scale)
  }

  def unary_- : ScaledInteger = {
    ScaledInteger(-n, scale)
  }

  def abs: ScaledInteger = ScaledInteger(n.abs, scale)

  def *(that: ScaledInteger): ScaledInteger = {
    ScaledInteger(n * that.n, scale * that.scale)
  }
}

class OffsetScaledIntegerType(val offset: Double, val scale: Double)
  extends UserDefinedType[OffsetScaledInteger]
  with MergeableType {

  require(scale > 0)

  override def sqlType: DataType = IntegerType

  override def serialize(obj: Any): Int = obj match {
    case OffsetScaledInteger(n, o, s) =>
      require(s == scale && o == offset); n
    case n: Int => n
  }

  override def deserialize(datum: Any): OffsetScaledInteger = {
    datum match {
      case n: Int => OffsetScaledInteger(n, offset, scale)
    }
  }

  override def defaultSize: Int = sqlType.defaultSize

  override def userClass: Class[OffsetScaledInteger] = classOf[OffsetScaledInteger]

  override def merge(that: DataType): DataType = that match {
    case t: ScaledIntegerType if scale == t.scale && offset == 0 => t
    case _: ScaledIntegerType => DoubleType
    case t: OffsetScaledIntegerType if scale == t.scale && offset == t.offset => this
    case _: OffsetScaledIntegerType => DoubleType
    case _: NumericType => DoubleType
  }

  override def mergeable(that: DataType): Boolean = that match {
    case _: ScaledIntegerType => true
    case _: OffsetScaledIntegerType => true
    case _: NumericType => true
    case _ => false
  }

  override def equals(other: Any): Boolean = other match {
    case that: OffsetScaledIntegerType =>
      that.scale == scale && that.offset == offset
    case _ => false
  }
}

class ScaledIntegerType(val scale: Double)
  extends UserDefinedType[ScaledInteger]
  with MergeableType {

  require(scale > 0)

  override def sqlType: DataType = IntegerType

  override def serialize(obj: Any): Int = obj match {
    case ScaledInteger(n, s) =>
      require(s == scale); n
    case n: Int => n
  }

  override def deserialize(datum: Any): ScaledInteger = {
    datum match {
      case n: Int => ScaledInteger(n, scale)
    }
  }

  override def defaultSize: Int = sqlType.defaultSize

  override def userClass: Class[ScaledInteger] = classOf[ScaledInteger]

  override def merge(that: DataType): DataType = that match {
    case t: ScaledIntegerType if scale == t.scale => this
    case _: ScaledIntegerType => DoubleType
    case t: OffsetScaledIntegerType if scale == t.scale && 0 == t.offset => ScaledIntegerType(scale)
    case _: OffsetScaledIntegerType => DoubleType
    case _: NumericType => DoubleType
  }

  override def mergeable(that: DataType): Boolean = that match {
    case _: ScaledIntegerType => true
    case _: OffsetScaledIntegerType => true
    case _: NumericType => true
    case _ => false
  }

  override def equals(other: Any): Boolean = other match {
    case that: ScaledIntegerType =>
      that.scale == scale
    case _ => false
  }
}

object OffsetScaledIntegerType extends AbstractDataType {
  def apply(offset: Double, scale: Double) = new OffsetScaledIntegerType(offset, scale)
  def unapply(t: DataType): Boolean = t.isInstanceOf[OffsetScaledIntegerType]
  override private[sql] def defaultConcreteType: DataType = IntegerType
  override private[sql] def simpleString: String = "offsetscaledint"
  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[OffsetScaledIntegerType]
}

object ScaledIntegerType extends AbstractDataType {
  def apply(scale: Double) = new ScaledIntegerType(scale)
  def unapply(t: DataType): Boolean = t.isInstanceOf[ScaledIntegerType]
  override private[sql] def defaultConcreteType: DataType = IntegerType
  override private[sql] def simpleString: String = "scaledint"
  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[ScaledIntegerType]
}
