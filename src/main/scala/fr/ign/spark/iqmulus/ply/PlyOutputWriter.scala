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

package fr.ign.spark.iqmulus.ply

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

class PlyOutputWriter(
  name: String,
  context: TaskAttemptContext,
  dataSchema: StructType,
  element: String,
  littleEndian: Boolean
)
    extends OutputWriter {

  private val file = {
    val path = getDefaultWorkFile(s"/1.$element")
    val fs = path.getFileSystem(context.getConfiguration)
    fs.create(path)
  }

  private var count = 0L

  private val schema = StructType(dataSchema.filterNot { Seq("fid", "pid") contains _.name })

  private val recordWriter = new RowOutputStream(new DataOutputStream(file), littleEndian, schema, dataSchema)

  def getDefaultWorkFile(extension: String): Path = {
    val uniqueWriteJobId = context.getConfiguration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId: TaskAttemptID = context.getTaskAttemptID
    val split = taskAttemptId.getTaskID.getId
    new Path(name, f"$split%05d-$uniqueWriteJobId$extension")
  }

  override def write(row: Row): Unit = {
    recordWriter.write(row)
    count += 1
  }

  override def close(): Unit = {
    recordWriter.close

    // write header
    val path = getDefaultWorkFile("/0.header")
    val fs = path.getFileSystem(context.getConfiguration)
    val dos = new java.io.DataOutputStream(fs.create(path))
    val header = new PlyHeader(path.toString, littleEndian, Map(element -> ((count, schema))))
    header.write(dos)
    dos.close

    // copy header and pdf to a final las file (1 per split)
    org.apache.hadoop.fs.FileUtil.copyMerge(
      fs, getDefaultWorkFile("/"),
      fs, getDefaultWorkFile(".ply"),
      true, context.getConfiguration, ""
    )
  }
}
