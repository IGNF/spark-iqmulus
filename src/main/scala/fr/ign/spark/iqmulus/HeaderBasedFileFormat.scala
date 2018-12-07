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

package fr.ign.spark.iqmulus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.hadoop.fs.Path

abstract class HeaderBasedFileFormat extends FileFormat {
  /*
  def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
files: Seq[FileStatus]): Option[StructType]) = {
throw new UnsupportedOperationException(s"inferSchema is not supported for $this")
}


  def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
dataSchema: StructType): OutputWriterFactory] = {
throw new UnsupportedOperationException(s"prepareWrite is not supported for $this")
}


  protected def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    throw new UnsupportedOperationException(s"buildReader is not supported for $this")
  }
*/

  override def isSplitable(
    sparkSession: SparkSession,
    options: Map[String, String],
    path: Path): Boolean = {
    true
  }
}
