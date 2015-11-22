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

class LasRelation(override val paths: Array[String])(@transient val sqlContext: SQLContext) extends BinarySectionRelation {

  lazy val headers: Array[LasHeader] = paths flatMap { location =>
    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration)
    try {
      val dis = fs.open(path)
      try LasHeader.read(location, dis)
      finally dis.close
    } catch {
      case _: java.io.FileNotFoundException =>
        logWarning(s"File not found : $location, skipping"); None
    }
  }

  override def sections: Array[BinarySection] = headers.map(_.toBinarySection)

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    new LasOutputWriterFactory(dataSchema)
  }
}

