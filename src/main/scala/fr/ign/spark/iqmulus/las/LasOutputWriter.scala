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

package fr.ign.spark.iqmulus.las

/*
import java.io.{OutputStream, IOException}
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util.HashMap

import org.apache.hadoop.fs.Path
import scala.collection.immutable.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{TaskAttemptID, RecordWriter, TaskAttemptContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.OutputWriter
 */
import org.apache.spark.sql.types._
import org.apache.hadoop.mapreduce.{ TaskAttemptID, RecordWriter, TaskAttemptContext }
import java.io.DataOutputStream
import org.apache.spark.sql.sources.OutputWriter
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.hadoop.io.{ NullWritable, BytesWritable }
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import java.text.NumberFormat
import org.apache.spark.sql.{ Row, SQLContext, sources }
import fr.ign.spark.iqmulus.RowOutputStream

/*
class LasOutputFormat(outputFile: Path) extends FileOutputFormat[NullWritable, BytesWritable] {  
  override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
    val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(context)
    val split = taskAttemptId.getTaskID.getId
    val name = FileOutputFormat.getOutputName(context)
    new Path(outputFile, s"$name-$split%05d-$uniqueWriteJobId$extension")
  }
}

class LasOutputWriter(path: String, dataSchema: StructType, context: TaskAttemptContext)
  extends OutputWriter {

  private val recordWriter: RecordWriter[NullWritable, BytesWritable] =
    new LasOutputFormat(new Path(path)).getRecordWriter(context)

  def toBytes(row: Row) : Array[Byte] = Array.empty[Byte]
  val writable = new BytesWritable

  override def write(row: Row): Unit = {
    val bytes = toBytes(row)
    writable.set(bytes,0,bytes.length)
    recordWriter.write(null, writable)
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }
}
 */
class LasOutputWriter(
  name: String,
  context: TaskAttemptContext,
  schema: StructType,
  format: Byte = 0,
  offset: Array[Double] = Array(0F, 0F, 0F),
  scale: Array[Double] = Array(1F, 1F, 1F)
)
    extends OutputWriter {

  private val file = {
    println(s"file : $name")
    println(context)
    println(s"file : $name")
    val path = getDefaultWorkFile(".las", "1.pdr")
    val fs = path.getFileSystem(context.getConfiguration)
    fs.create(path)
  }

  private val pmin = Array.fill[Double](3)(Double.PositiveInfinity)
  private val pmax = Array.fill[Double](3)(Double.NegativeInfinity)
  private val countByReturn = Array.fill[Long](15)(0)

  private def header = {
    val count = countByReturn.sum
    new LasHeader(name, format, count, pmin, pmax, scale, offset, countByReturn)
  }

  private val recordWriter = new RowOutputStream(new DataOutputStream(file), littleEndian = true, schema)

  def getDefaultWorkFile(extension: String, section: String): Path = {
    val uniqueWriteJobId = context.getConfiguration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId: TaskAttemptID = context.getTaskAttemptID
    val split = taskAttemptId.getTaskID.getId
    new Path(name, f"part-r-$split%05d-$uniqueWriteJobId$extension/$section")
  }

  override def write(row: Row): Unit = {
    recordWriter.write(row)
    val x = offset(0) + scale(0) * row.getAs[Int]("x").toDouble
    val y = offset(1) + scale(1) * row.getAs[Int]("y").toDouble
    val z = offset(2) + scale(2) * row.getAs[Int]("z").toDouble
    val ret = row.getAs[Byte]("flags") & 0x3
    countByReturn(ret) += 1
    pmin(0) = Math.min(pmin(0), x)
    pmin(1) = Math.min(pmin(1), y)
    pmin(2) = Math.min(pmin(2), z)
    pmax(0) = Math.max(pmax(0), x)
    pmax(1) = Math.max(pmax(1), y)
    pmax(2) = Math.max(pmax(2), z)
  }

  override def close(): Unit = {
    recordWriter.close

    println(header)
    val path = getDefaultWorkFile(".las", "0.header")
    val fs = path.getFileSystem(context.getConfiguration)
    val dos = new java.io.DataOutputStream(fs.create(path))
    header.write(dos)
    dos.close
  }
}

/*
// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[las] class LasOutputWriter(
  path: String, dataSchema: StructType, context: TaskAttemptContext) 
  extends OutputWriter {

  /**
 * Overrides the couple of methods responsible for generating the output streams / files so
 * that the data can be correctly partitioned
 * */
  /*
  private val recordWriter: RecordWriter[NullWritable, BytesWritable] = {
    val dos = new DataOutputStream(path)
    LasHeader.schema(format).write(dos)

  }
 */
  private val format = 0

  private val recordWriter: RecordWriter[NullWritable, BytesWritable] = {.getRecordWriter(context)
  }


  /*
    new AvroKeyOutputFormat[GenericRecord]() {

      private def getConfigurationFromContext(context: TaskAttemptContext): Configuration = {
        // Use reflection to get the Configuration. This is necessary because TaskAttemptContext
        // is a class in Hadoop 1.x and an interface in Hadoop 2.x.
        val method = context.getClass.getMethod("getConfiguration")
        method.invoke(context).asInstanceOf[Configuration]
      }

      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val uniqueWriteJobId =
          getConfigurationFromContext(context).get("spark.sql.sources.writeJobUUID")
        val taskAttemptId: TaskAttemptID = {
          // Use reflection to get the TaskAttemptID. This is necessary because TaskAttemptContext
          // is a class in Hadoop 1.x and an interface in Hadoop 2.x.
          val method = context.getClass.getMethod("getTaskAttemptID")
          method.invoke(context).asInstanceOf[TaskAttemptID]
        }
        val split = taskAttemptId.getTaskID.getId
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
      }

      @throws(classOf[IOException])
      override def getAvroFileOutputStream(c: TaskAttemptContext): OutputStream = {
        val path = getDefaultWorkFile(context, ".avro")
        path.getFileSystem(getConfigurationFromContext(context)).create(path)
      }

    }.getRecordWriter(context)
 */
  override def write(row: Row): Unit = recordWriter write row

  override def close(): Unit = recordWriter close

}
 */

/*

package fr.ign.spark.iqmulus

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import java.io.DataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

package object las {

  implicit class LASDataFrame(df: DataFrame) {
    val defaultAliases : Map[String,String] = Map[String,String]()
    def saveAsLasFile(location: String, format : Byte,
      aliases : Map[String,String] = defaultAliases) = {
      val names = aliases
      val schema = LASHeader.schema(format) // no user types for now
      val cols   = schema.fieldNames.intersect(df.schema.fieldNames)
      val partitionSize = df.rdd.mapPartitionsWithIndex { case (pid, iter) =>
        Iterator((pid, iter.size)) }.collect.toMap
      df.select(cols.head, cols.tail :_*).rdd.mapPartitionsWithIndex({
        case (pid, iter) =>
          val name = s"$location/$pid.las"
          val path = new Path(name)
          val fs = path.getFileSystem(new Configuration)
          val f = fs.create(path)
          val count = partitionSize(pid)
          val header = new LASHeader(name,count,format)
          val dos = new DataOutputStream(f);
          header.write(dos)
          iter.foreach(dos write _)
          dos.close
          Iterator((path.toString, count))
      }, true)
    }

  }

}

 */

