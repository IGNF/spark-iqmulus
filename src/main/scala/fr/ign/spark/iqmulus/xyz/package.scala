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
import org.apache.spark.sql.types.{ FloatType, StructType }

package object xyz {

  /**
   * Adds a method, `xyz`, to DataFrameWriter that allows you to write xyz files using
   * the DataFileWriter
   */
  implicit class XyzDataFrameWriter(writer: DataFrameWriter) {
    def xyz: String => Unit = writer.format("fr.ign.spark.iqmulus.xyz").save
  }

  /**
   * Adds a method, `xyz`, to DataFrameReader that allows you to read xyz files using
   * the DataFileReade
   */
  implicit class XyzDataFrameReader(reader: DataFrameReader) {
    def xyz: String => DataFrame = reader.format("fr.ign.spark.iqmulus.xyz").load
  }

  implicit class XyzDataFrame(df: DataFrame) {
    def saveAsXyz(location: String) = {
      val df_id = df.drop("id")
      require(df_id.schema.fieldNames.take(3) sameElements Array("x", "y", "z"))
      require(df_id.schema.fields.map(_.dataType).take(3).forall(_ == FloatType))
      val saver = (key: Int, iter: Iterator[Row]) => Iterator(iter.saveXyz(s"$location/$key.xyz"))
      df_id.rdd.mapPartitionsWithIndex(saver, true).collect
    }
  }

  implicit class XyzRowIterator(iter: Iterator[Row]) {
    def saveXyz(filename: String) = {
      val path = new org.apache.hadoop.fs.Path(filename)
      val fs = path.getFileSystem(new org.apache.hadoop.conf.Configuration)
      val f = fs.create(path)
      val dos = new java.io.DataOutputStream(f)
      var count = 0L
      iter.foreach(row => { count += 1; dos.writeBytes(row.mkString("", "\t", "\n")) })
      dos.close
      (filename, count)
    }
  }
}

