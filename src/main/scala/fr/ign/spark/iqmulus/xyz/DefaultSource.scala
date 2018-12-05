
/*
 * Copyright 2015-2017 IGN
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

package fr.ign.spark.iqmulus.xyz

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.IOException
import scala.util.control.NonFatal
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.TextBasedFileFormat
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.HadoopFileLinesReader
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        throw e
      case NonFatal(e) =>
        throw new IOException(e)
    }
  }

  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}

object XyzFileFormat {

  val xyzrgbSchema = StructType(Array(
    StructField("x", FloatType, false),
    StructField("y", FloatType, false),
    StructField("z", FloatType, false),
    StructField("r", ByteType, false),
    StructField("g", ByteType, false),
    StructField("b", ByteType, false)))

  val xyzSchema = StructType(Array(
    StructField("x", FloatType, false),
    StructField("y", FloatType, false),
    StructField("z", FloatType, false)))

}

/**
 * Provides access to XYZ data from pure SQL statements.
 */
class DefaultSource extends TextBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "xyz"

  override def toString: String = "XYZ"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[DefaultSource]

  override def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]): Option[StructType] = options.getOrElse("format", "xyz") match {
    case "xyzrgb" => Some(XyzFileFormat.xyzrgbSchema)
    case "xyz" => Some(XyzFileFormat.xyzSchema)
    case _ => None
  }

  override def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = {
    new OutputWriterFactory {
      override def newInstance(
        path: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
        new XyzOutputWriter(path, dataSchema, context)
      }
      override def getFileExtension(context: TaskAttemptContext): String = ".xyz"
    }
  }

  override def buildReader(
    sparkSession: SparkSession,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value
      val linesReader = new HadoopFileLinesReader(file, conf)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))
      val dataTypes = dataSchema.fields.map(_.dataType)
      linesReader.map { line =>
        InternalRow.fromSeq((line.toString.trim.split("\t") zip dataTypes).map {
          case (x, StringType) => x
          case (x, ByteType) => x.toByte
          case (x, ShortType) => x.toShort
          case (x, IntegerType) => x.toInt
          case (x, LongType) => x.toLong
          case (x, FloatType) => x.toFloat
          case (x, DoubleType) => x.toDouble
          case _ => null
        }.padTo(dataTypes.size, null))
      }
    }
  }

}

private[xyz] class XyzOutputWriter(
  filename: String,
  dataSchema: StructType,
  context: TaskAttemptContext)
  extends OutputWriter {

  private lazy val dos = {
    val path = new org.apache.hadoop.fs.Path(filename)
    val fs = path.getFileSystem(context.getConfiguration)
    val f = fs.create(path)
    new java.io.DataOutputStream(f)
  }

  override def write(row: InternalRow): Unit = {
    dos.writeBytes(row.toSeq(dataSchema).mkString("", "\t", "\n"))
  }

  override def close(): Unit = dos.close
}

