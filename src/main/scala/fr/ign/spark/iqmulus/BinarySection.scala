/*
 * Copyright 2015-2019 IGN
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

package fr.ign.spark.iqmulus

import java.nio.{ ByteBuffer, ByteOrder }
import org.apache.hadoop.io._
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types._
import org.apache.spark.rdd.{ NewHadoopPartition, NewHadoopRDD, RDD, UnionRDD }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.{ FileSystem, FileStatus, Path }
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Expression
// import org.apache.spark.Logging

// workaround the protected Cast nullSafeEval to make it public
class IQmulusCast(child: Expression, dataType: DataType)
  extends Cast(child, dataType) {
  override def nullSafeEval(input: Any): Any = super.nullSafeEval(input)
}

case class BinarySection(
  location: String,
  var offset: Long,
  count: Long,
  littleEndian: Boolean,
  private val _schema: StructType,
  private val _stride: Int = 0,
  pidName: String = "pid",
  fidName: String = "fid") {

  val sizes = _schema.fields.map(_.dataType.defaultSize)
  val offsets = sizes.scanLeft(0)(_ + _)
  val length = offsets.last
  val stride = if (_stride > 0) _stride else length
  def size: Long = stride * count
  val schema = StructType(StructField(fidName, IntegerType, false) +: StructField(pidName, LongType, false) +: _schema.fields)

  val fieldOffsetMap: Map[String, (StructField, Int)] =
    (_schema.fields zip offsets).map(x => (x._1.name, x)).toMap.withDefault(name => (StructField(name, NullType, nullable = true), -1))

  def order = if (littleEndian) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN
  def toBuffer(bytes: BytesWritable): ByteBuffer = ByteBuffer.wrap(bytes.getBytes).order(order)

  def bytesSeq(fid: Int, prop: Seq[(DataType, (StructField, Int))]): Seq[(Long, ByteBuffer) => Any] =
    prop map {
      case (LongType, (StructField(name, _, _, _), _)) if name == pidName => (pid: Long, bytes: ByteBuffer) => pid
      case (targetType, (StructField(name, _, _, _), _)) if name == pidName => (pid: Long, bytes: ByteBuffer) => {
        new IQmulusCast(Literal(pid), targetType).nullSafeEval(pid)
      }
      case (LongType, (StructField(name, _, _, _), _)) if name == fidName => (pid: Long, bytes: ByteBuffer) => fid
      case (targetType, (StructField(name, _, _, _), _)) if name == fidName => (pid: Long, bytes: ByteBuffer) => {
        new IQmulusCast(Literal(fid), targetType).nullSafeEval(fid)
      }
      case (targetType, (sourceField, offset)) => {
        val getValue = sourceField.get(offset)
        val md = sourceField.metadata
        val nodata = sourceField.nullable && sourceField.metadata.contains("nodata")
        def getNullableValue = if (!nodata) getValue else {
          val nodataval: Any = sourceField.dataType match {
            case BooleanType => md.getBoolean("nodata")
            case ByteType => md.getLong("nodata").toByte
            case ShortType => md.getLong("nodata").toShort
            case IntegerType => md.getLong("nodata").toInt
            case LongType => md.getLong("nodata")
            case FloatType => md.getDouble("nodata").toFloat
            case DoubleType => md.getDouble("nodata")
          }
          (bytes: ByteBuffer) => { val v = getValue(bytes); if (v == nodataval) null else v }
        }
        sourceField.dataType match {
          case NullType => (_: Long, bytes: ByteBuffer) => getValue(bytes)
          case sourceType if sourceType == targetType => (_: Long, bytes: ByteBuffer) => getNullableValue(bytes)
          case sourceType => (_: Long, bytes: ByteBuffer) => {
            val value = getNullableValue(bytes)
            new IQmulusCast(Literal(value), targetType).nullSafeEval(value)
          }
        }
      }
    }

  def getSeqAux(fid: Int, prop: Seq[(DataType, (StructField, Int))])(key: LongWritable, bytes: BytesWritable): Seq[Any] =
    {
      (bytesSeq(fid, prop) map (_(key.get, toBuffer(bytes))))
    }

  def getSubSeq(fid: Int, dataSchema: StructType, requiredColumns: Array[String]) = {
    val fieldsWithOffsets = dataSchema.map(field => (field.dataType, fieldOffsetMap(field.name)))
    /*
    val requiredFieldsWithOffsets = fieldsWithOffsets filter {
      case (_, (f, _)) => (requiredColumns contains f.name)
    }
    */
    val requiredFieldsWithOffsets = requiredColumns map (col => fieldsWithOffsets.find(_._2._1.name == col).get)
    getSeqAux(fid, requiredFieldsWithOffsets) _
  }
}
