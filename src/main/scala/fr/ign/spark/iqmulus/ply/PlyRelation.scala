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

import fr.ign.spark.iqmulus.{ BinarySectionRelation, BinarySection }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.{ FileOutputFormat, FileOutputCommitter }
import org.apache.spark.sql.sources.OutputWriterFactory
import org.apache.hadoop.mapreduce.{ TaskAttemptContext, JobContext }
import org.apache.spark.deploy.SparkHadoopUtil
import scala.util.{ Try, Success, Failure }
import org.apache.spark.Logging

class PlyOutputCommitter(
    name: Path,
    context: TaskAttemptContext
) extends FileOutputCommitter(name, context) with Logging {

  override def commitJob(job: JobContext) = {
    super.commitJob(job);

    val conf = job.getConfiguration
    val outputFiles = conf.getStrings("fr.ign.spark.iqmulus.outputFiles").map(new Path(_).getName)
    val outputFilesCol = conf.get("fr.ign.spark.iqmulus.outputFilesCol")

    val fs = name.getFileSystem(conf)
    val util = SparkHadoopUtil.get
    for (path <- util.listLeafDirStatuses(fs, name).map(_.getPath)) {
      // get all headers
      val paths = util.globPath(new Path(path, "*.ply.header"))

      val header = {
        // read all headers
        val headers = paths.flatMap(path => Try(PlyHeader.read(path)) match {
          case Success(h) => Some(h)
          case Failure(e) => logWarning(s"Skipping $path : e.getMessage"); None
        })
        // assume all headers have been read successfully
        require(headers.length == paths.length)
        headers.reduce(_ merge _)
      }
      val headerPath = new Path(path, ".header")
      val dos = new java.io.DataOutputStream(fs.create(headerPath))
      header.write(dos)
      dos.close

      def replace(path: Path): Path = if (path.isRoot) path else {
        val split = path.getName.split('=')
        new Path(
          replace(path.getParent),
          if (split.length != 2 || split(0) != outputFilesCol) path.getName
          else try { outputFiles(split(1).takeWhile(_.isDigit).toInt) } catch { case _: java.lang.NumberFormatException => path.getName }
        )
      }
      val elementStatuses = header.elements.map(_.name).flatMap(el => fs.globStatus(new Path(path, s"*.ply.$el")))
      fr.ign.spark.iqmulus.copyMerge(
        fs, Array(header.toString()) ++ elementStatuses,
        fs, replace(path.suffix(".ply")),
        false, conf
      )
      fs.delete(path, true)
    }
  }
}

class PlyOutputFormat extends FileOutputFormat {
  override def getOutputCommitter(context: TaskAttemptContext) =
    new PlyOutputCommitter(FileOutputFormat.getOutputPath(context), context)
  def getRecordWriter(context: TaskAttemptContext) = ???
}

class PlyRelation(
  override val paths: Array[String],
  override val maybeDataSchema: Option[StructType],
  override val userDefinedPartitionColumns: Option[StructType],
  parameters: Map[String, String]
)(@transient val sqlContext: SQLContext)
    extends BinarySectionRelation(parameters) {

  val element = parameters.getOrElse("element", "vertex")
  val littleEndian = parameters.getOrElse("littleEndian", "true").toBoolean

  lazy val headers: Array[PlyHeader] = paths flatMap { location =>
    Try {
      val path = new Path(location)
      val fs = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration)
      val dis = fs.open(path)
      try PlyHeader.read(location, dis)
      finally {
        dis.close
        fs.close
      }
    } match {
      case Success(h) => Some(h)
      case Failure(e) => logWarning(s"Skipping $location : ${e.getMessage}"); None
    }
  }

  override def sections: Array[BinarySection] =
    headers.flatMap(_.section.get(element))

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {

    job.setOutputFormatClass(classOf[PlyOutputFormat])

    // look for the outputFile column in all schemas
    val col = (userDefinedPartitionColumns ++ maybeDataSchema ++ Some(dataSchema))
      .flatMap(_.find(_.name == "fid")).headOption
    val outputFiles = col.map(_.metadata.getStringArray("paths")).getOrElse(Array.empty[String])
    job.getConfiguration.setStrings("fr.ign.spark.iqmulus.outputFiles", outputFiles: _*)
    job.getConfiguration.set("fr.ign.spark.iqmulus.outputFilesCol", "fid")
    new PlyOutputWriterFactory(element, littleEndian)
  }
}

