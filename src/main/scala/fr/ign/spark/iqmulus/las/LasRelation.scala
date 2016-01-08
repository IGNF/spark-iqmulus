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

import fr.ign.spark.iqmulus.{ BinarySectionRelation, BinarySection }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.OutputWriterFactory
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.types._
import scala.util.{ Try, Success, Failure }

class LasRelation(
  override val paths: Array[String],
  override val maybeDataSchema: Option[StructType],
  override val userDefinedPartitionColumns: Option[StructType],
  parameters: Map[String, String]
)(@transient val sqlContext: SQLContext)
    extends BinarySectionRelation(parameters) {

  def format = parameters.get("lasformat").map(_.toByte)
  def minor = parameters.get("minor").map(_.toByte).getOrElse(Version.minorDefault)
  def major = parameters.get("major").map(_.toByte).getOrElse(Version.majorDefault)
  def version = parameters.get("version").map(Version.fromString)
    .getOrElse(Version(major, minor))

  lazy val headers: Array[LasHeader] = paths flatMap { location =>
    Try {
      val path = new Path(location)
      val fs = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration)
      val dis = fs.open(path)
      try LasHeader.read(location, dis)
      finally {
        dis.close
        fs.close
      }
    } match {
      case Success(h) => Some(h)
      case Failure(e) => logWarning(s"Skipping $location : ${e.getMessage}"); None
    }
  }

  override def sections: Array[BinarySection] = headers.map(_.toBinarySection)

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    new LasOutputWriterFactory(format, version)
  }
}

