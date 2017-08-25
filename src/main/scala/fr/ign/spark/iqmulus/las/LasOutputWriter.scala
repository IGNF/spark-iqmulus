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

import org.apache.spark.sql.types._
import org.apache.hadoop.mapreduce.{ TaskAttemptID, RecordWriter, TaskAttemptContext }
import java.io.DataOutputStream
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.hadoop.io.{ NullWritable, BytesWritable }
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import java.text.NumberFormat
import org.apache.spark.sql.{ Row, SQLContext, sources }
import fr.ign.spark.iqmulus.RowOutputStream

class LasOutputWriter(
  name: String,
  context: TaskAttemptContext,
  dataSchema: StructType,
  formatOpt: Option[Byte] = None,
  version: Version = Version(),
  offset: Array[Double] = Array(0F, 0F, 0F),
  scale: Array[Double] = Array(0.01F, 0.01F, 0.01F))
  extends OutputWriter {

  private val file = {
    val path = getDefaultWorkFile("/1.pdr")
    val fs = path.getFileSystem(context.getConfiguration)
    fs.create(path)
  }

  private val pmin = Array.fill[Double](3)(Double.PositiveInfinity)
  private val pmax = Array.fill[Double](3)(Double.NegativeInfinity)
  private val countByReturn = Array.fill[Long](15)(0)
  private def count = countByReturn.sum

  private val format = formatOpt.getOrElse(LasHeader.formatFromSchema(dataSchema))

  // todo, extra bytes
  private val schema = LasHeader.schema(format)
  private def header =
    new LasHeader(name, format, count, pmin, pmax, scale, offset, countByReturn)

  private val recordWriter = new RowOutputStream(new DataOutputStream(file), littleEndian = true, schema, dataSchema)

  def getDefaultWorkFile(extension: String): Path = {
    val uniqueWriteJobId = context.getConfiguration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId: TaskAttemptID = context.getTaskAttemptID
    val split = taskAttemptId.getTaskID.getId
    new Path(name, f"$split%05d-$uniqueWriteJobId$extension")
  }

  override def write(row: Row): Unit = {
    recordWriter.write(row)

    // gather statistics for the header
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

    // write header
    val path = getDefaultWorkFile("/0.header")
    val fs = path.getFileSystem(context.getConfiguration)
    val dos = new java.io.DataOutputStream(fs.create(path))
    header.write(dos)
    dos.close

    // copy header and pdf to a final las file (1 per split)
    org.apache.hadoop.fs.FileUtil.copyMerge(
      fs, getDefaultWorkFile("/"),
      fs, getDefaultWorkFile(".las"),
      true, context.getConfiguration, "")
  }
}
