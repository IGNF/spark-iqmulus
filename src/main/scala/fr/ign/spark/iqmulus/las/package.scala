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

package fr.ign.spark.iqmulus

import org.apache.spark.sql.{SQLContext, DataFrameReader, DataFrameWriter, DataFrame}

package object las {

  /**
   * Adds a method, `las`, to DataFrameWriter that allows you to write las files using
   * the DataFileWriter
   */
  implicit class LasDataFrameWriter(writer: DataFrameWriter) {
    def las: String => Unit = writer.format("fr.ign.spark.iqmulus.las").save
  }

  /**
   * Adds a method, `las`, to DataFrameReader that allows you to read las files using
   * the DataFileReade
   */
  implicit class LasDataFrameReader(reader: DataFrameReader) {
    def las: String => DataFrame = reader.format("fr.ign.spark.iqmulus.las").load
  }
  
}

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
    def saveAsLasFile(location: String, format : Byte, aliases : Map[String,String] = defaultAliases) = {
      val names = aliases
      val schema = LASHeader.schema(format) // no user types for now
      val cols   = schema.fieldNames.intersect(df.schema.fieldNames)
      val partitionSize = df.rdd.mapPartitionsWithIndex { case (pid, iter) => Iterator((pid, iter.size)) }.collect.toMap
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

