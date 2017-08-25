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

import org.apache.spark.sql.{ SQLContext, DataFrameReader, DataFrameWriter, DataFrame, Row }
import org.apache.spark.sql.types.StructType

package object ply {

  /**
   * Adds a method, `ply`, to DataFrameWriter that allows you to write ply files using
   * the DataFileWriter
   */
  implicit class PlyDataFrameWriter(writer: DataFrameWriter) {
    def ply: String => Unit = writer.format("fr.ign.spark.iqmulus.ply").save
  }

  /**
   * Adds a method, `ply`, to DataFrameReader that allows you to read ply files using
   * the DataFileReade
   */
  implicit class PlyDataFrameReader(reader: DataFrameReader) {
    def ply: String => DataFrame = reader.format("fr.ign.spark.iqmulus.ply").load
  }

  implicit class PlyDataFrame(df: DataFrame) {
    def saveAsPly(location: String, littleEndian: Boolean = true) = {
      val df_id = df.drop("pid").drop("fid")
      val schema = df_id.schema
      val saver = (key: Int, iter: Iterator[Row]) =>
        Iterator(iter.saveAsPly(s"$location/$key.ply", schema, littleEndian))
      df_id.rdd.mapPartitionsWithIndex(saver, true).collect
    }
  }

  implicit class PlyRowIterator(iter: Iterator[Row]) {
    def saveAsPly(
      filename: String,
      schema: StructType,
      littleEndian: Boolean) = {
      val path = new org.apache.hadoop.fs.Path(filename)
      val fs = path.getFileSystem(new org.apache.hadoop.conf.Configuration)
      val f = fs.create(path)
      val rows = iter.toArray
      val count = rows.size.toLong
      val header = new PlyHeader(filename, littleEndian, Map("vertex" -> ((count, schema))))
      val dos = new java.io.DataOutputStream(f);
      dos.write(header.toString.getBytes)
      val ros = new RowOutputStream(dos, littleEndian, schema)
      rows.foreach(ros.write)
      dos.close
      header
    }
  }
}

