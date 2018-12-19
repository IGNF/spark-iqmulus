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

package fr.ign.spark.iqmulus.ply

import scala.util.{ Try, Success, Failure }

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.Filter

import fr.ign.spark.iqmulus.HeaderBasedFileFormat
import fr.ign.spark.iqmulus.BinarySection
import fr.ign.spark.iqmulus.MergeableStructType
import fr.ign.spark.iqmulus.SerializableConfiguration

class PlyFileFormat extends HeaderBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "ply"

  override def toString: String = "PLY"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[PlyFileFormat]

  def sections(headers: Seq[PlyHeader], options: PlyOptions): Seq[BinarySection] =
    headers.flatMap(_.section.get(options.element))

  def header(sparkSession: SparkSession, file: FileStatus): Option[PlyHeader] = {
    Try {
      val path = file.getPath
      val location = path.toString
      val fs = FileSystem.get(path.toUri, sparkSession.sparkContext.hadoopConfiguration)
      val dis = fs.open(path)
      try PlyHeader.read(location, dis)
      finally {
        dis.close
        fs.close
      }
    } match {
      case Success(h) => Some(h)
      case Failure(e) => None
    }
  }

  override def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]): Option[StructType] = {
    val plyOptions = new PlyOptions(options)

    val s = sections(files.flatMap(file => header(sparkSession, file)), plyOptions)

    Some(if (s.isEmpty) StructType(Nil) else s.map(_.schema).reduce(_ merge _))
    /*.map(
         field => if (field.name == "fid") {
           val builder = new MetadataBuilder().withMetadata(field.metadata)
           val metadata = builder.putStringArray("paths", paths).build
           field.copy(metadata = metadata)
         } else field))
         */
  }

  override def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = ???
  /*{
    val plyOptions = new PlyOptions(options)
    new OutputWriterFactory {
      override def newInstance(
        path: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
        new PlyOutputWriter(path, context, dataSchema, plyOptions)
      }
      override def getFileExtension(context: TaskAttemptContext): String = ".ply"
    }
  }
  */

  override def buildReader(
    sparkSession: SparkSession,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val plyOptions = new PlyOptions(options)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val broadcastedOptions =
      sparkSession.sparkContext.broadcast(plyOptions)

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value
      val options = broadcastedOptions.value
      val origin = file.filePath
      val path = new Path(origin)
      val fs = path.getFileSystem(broadcastedHadoopConf.value.value)

      var splitStart = Math.max(file.start, options.recordOffset)
      splitStart = recordOffset + (((splitStart - recordOffset) / recordStride) * recordStride).toLong
      // splitEnd byte marker that the fileSplit ends at
      var splitEnd = recordOffset + recordCount * recordStride
      splitEnd = Math.min(file.start + file.length, splitEnd)
      splitEnd = recordOffset + (((splitEnd - recordOffset) / recordStride) * recordStride).toLong

      reader.initialize(FileSplit)
      new RecordReaderIterator(reader)
    }
  }

}
