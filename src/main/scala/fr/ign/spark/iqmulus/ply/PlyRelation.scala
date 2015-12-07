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

class PlyOutputCommitter(
    name: Path,
    context: TaskAttemptContext
) extends FileOutputCommitter(name, context) {

  override def commitJob(job: JobContext) = {
    super.commitJob(job);

    val conf = job.getConfiguration
    val inputFiles = conf.getStrings("fr.ign.spark.iqmulus.inputFiles").map(new Path(_).getName)
    val inputCol = conf.get("fr.ign.spark.iqmulus.inputCol")

    println(inputCol)
    inputFiles foreach println

    val fs = name.getFileSystem(conf)
    val util = SparkHadoopUtil.get
    for (path <- util.listLeafDirStatuses(fs, name).map(_.getPath)) {
      // get all headers
      val pattern = new Path(path, "*.ply.header")
      val paths = util.globPath(pattern)
      // read all headers
      val headers = paths.flatMap(PlyHeader.read(_))
      // unreadable headers are not tackled yet
      require(headers.length == paths.length)
      // delete .ply.header files
      paths.foreach(fs.delete(_, false))

      val header = headers.reduce(_ merge _)
      val headerPath = new Path(path, ".header")
      val dos = new java.io.DataOutputStream(fs.create(headerPath))
      header.write(dos)
      dos.close

      def replace(path: Path): Path = if (path.isRoot) path else {
        val split = path.getName.split('=')
        new Path(
          replace(path.getParent),
          if (split.length != 2 || split(0) != inputCol) path.getName
          else try { inputFiles(split(1).takeWhile(_.isDigit).toInt) } catch { case _: java.lang.NumberFormatException => path.getName }
        )
      }

      fr.ign.spark.iqmulus.copyMerge(
        fs, fs.listStatus(headerPath) ++ fs.globStatus(new Path(path, "*.ply.*")),
        fs, replace(path.suffix(".ply")),
        false, conf, ""
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
    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration)
    try {
      val dis = fs.open(path)
      try PlyHeader.read(location, dis)
      finally dis.close
    } catch {
      case _: java.io.FileNotFoundException =>
        logWarning(s"File not found : $location, skipping"); None
    }
  }

  override def sections: Array[BinarySection] =
    headers.flatMap(_.section.get(element))

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    job.setOutputFormatClass(classOf[PlyOutputFormat])

    // TODO: get the inputFiles from the dataframe, possibly using the column metadata
    val inputFiles = Array("file:/test1/test2.ply", "file:/aaa/bbb/ccc.ply")
    job.getConfiguration.setStrings("fr.ign.spark.iqmulus.inputFiles", inputFiles: _*)
    job.getConfiguration.set("fr.ign.spark.iqmulus.inputCol", "fid")
    new PlyOutputWriterFactory(element, littleEndian)
  }
}

