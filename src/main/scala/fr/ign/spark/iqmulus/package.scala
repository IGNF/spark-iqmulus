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

package fr.ign.spark


import org.apache.spark.sql.{ Row, SQLContext, DataFrame }

import org.apache.spark.rdd.UnionRDD
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.types._
import java.nio.{ ByteBuffer, ByteOrder }
import java.io.DataOutputStream
import org.apache.hadoop.conf.Configuration

// merge workaround imports
import org.apache.spark.SparkException
import scala.collection.mutable.ArrayBuffer

package object iqmulus {

	/**
	 * http://polyglot-window.blogspot.com/2009/03/arm-blocks-in-scala-revisited.html
	 * http://stackoverflow.com/questions/2207425/what-automatic-resource-management-alternatives-exists-for-scala
	 */
	object using {
		def apply[A <: { def close(): Unit }, B](r: A)(f: A => B): B = try {
			f(r)
		} finally {
			import scala.language.reflectiveCalls
			r.close()
		}
	}

  
  // adapted from private method in StructType object
  // added nullable=true when a field is present on 1 side only
	object MergeableStructType {

		private[iqmulus] def merge(left: DataType, right: DataType): DataType =
				(left, right) match {
				case (ArrayType(leftElementType, leftContainsNull),
						ArrayType(rightElementType, rightContainsNull)) =>
				ArrayType(
						merge(leftElementType, rightElementType),
						leftContainsNull || rightContainsNull)

				case (MapType(leftKeyType, leftValueType, leftContainsNull),
						MapType(rightKeyType, rightValueType, rightContainsNull)) =>
				MapType(
						merge(leftKeyType, rightKeyType),
						merge(leftValueType, rightValueType),
						leftContainsNull || rightContainsNull)

				case (leftStruct @ StructType(_), rightStruct @ StructType(_)) =>
				merge(leftStruct,rightStruct)
				/* // commented out because DecimalType.Fixed is private
      case (DecimalType.Fixed(leftPrecision, leftScale),
        DecimalType.Fixed(rightPrecision, rightScale)) =>
        if ((leftPrecision == rightPrecision) && (leftScale == rightScale)) {
          DecimalType(leftPrecision, leftScale)
        } else if ((leftPrecision != rightPrecision) && (leftScale != rightScale)) {
          throw new SparkException("Failed to merge Decimal Tpes with incompatible " +
            s"precision $leftPrecision and $rightPrecision & scale $leftScale and $rightScale")
        } else if (leftPrecision != rightPrecision) {
          throw new SparkException("Failed to merge Decimal Tpes with incompatible " +
            s"precision $leftPrecision and $rightPrecision")
        } else {
          throw new SparkException("Failed to merge Decimal Tpes with incompatible " +
            s"scala $leftScale and $rightScale")
        }
				 */
				case (leftUdt: UserDefinedType[_], rightUdt: UserDefinedType[_])
				if leftUdt.userClass == rightUdt.userClass => leftUdt

				case (leftType, rightType) if leftType == rightType =>
				leftType

				case _ =>
				throw new SparkException(s"Failed to merge incompatible data types $left and $right")
		}

		private[iqmulus] def merge(left: StructType, right: StructType): StructType = {
				val newFields = ArrayBuffer.empty[StructField]

						val rightMapped = fieldsMap(right.fields)
						left.fields.foreach {
						case leftField @ StructField(leftName, leftType, leftNullable, _) =>
						rightMapped.get(leftName)
						.map { case rightField @ StructField(_, rightType, rightNullable, _) =>
						leftField.copy(
								dataType = merge(leftType, rightType),
								nullable = leftNullable || rightNullable)
						}
						.orElse(Some(asNullable(leftField)))
						.foreach(newFields += _)
				}

				val leftMapped = fieldsMap(left.fields)
						right.fields
						.filterNot(f => leftMapped.get(f.name).nonEmpty)
						.foreach(newFields += asNullable(_))

						StructType(newFields)
		}

    private[iqmulus] def asNullable(field: StructField): StructField =
      StructField(field.name,field.dataType,true,field.metadata)
    
		private[iqmulus] def fieldsMap(fields: Array[StructField]): Map[String, StructField] = {
				import scala.collection.breakOut
				fields.map(s => (s.name, s))(breakOut)
		}
	}
	implicit class MergeableStructType(left: StructType) {
		def merge(right: StructType): StructType = MergeableStructType.merge(left,right)
				/*
    // access private[sql] method : https://gist.github.com/jorgeortiz85/908035
    def merge(right: StructType): StructType = {
      val method = StructType.getClass.getDeclaredMethods.find(_.getName == "merge").get
      method.setAccessible(true)
      method.invoke(left, right).asInstanceOf[StructType]
      //left merge right
    }
				 */
	}

	/*
  implicit class DataTypeWithSize(dataType: DataType) {
    def size: Byte = dataType match {
      case ByteType => 1
      case ShortType => 2
      case IntegerType => 4
      case LongType => 8
      case FloatType => 4
      case DoubleType => 8
      case other => sys.error(s"Unsupported type $other")
    }
  }
	 */
	implicit class StructFieldWithGet(field: StructField) {
		def get(offset: Int): (ByteBuffer => Any) = field.dataType match {
		case ByteType => b => b get offset
		case ShortType => b => b getShort offset
		case IntegerType => b => b getInt offset
		case LongType => b => b getLong offset
		case FloatType => b => b getFloat offset
    case DoubleType => b => b getDouble offset
    case NullType => b => null
		case other => sys.error(s"Unsupported type $other")
		}
	}

	implicit class RowOutputStream(dos: DataOutputStream) {
		def write(row: Row) = {
			val typeValue = row.toSeq zipAll (row.schema.fields.map(_.dataType), 0, ByteType)
					typeValue.foreach {
					case (v, ByteType) => dos writeByte v.asInstanceOf[Byte]
					case (v, ShortType) => dos writeShort v.asInstanceOf[Short]
					case (v, IntegerType) => dos writeInt v.asInstanceOf[Int]
					case (v, LongType) => dos writeLong v.asInstanceOf[Long]
					case (v, FloatType) => dos writeFloat v.asInstanceOf[Float]
          case (v, DoubleType) => dos writeDouble v.asInstanceOf[Double]
          case (v, NullType) => ()
					case (_, other) => sys.error(s"Unsupported type $other")
			}
		}
	}
}

