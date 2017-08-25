/*
 * Copyright 2015-2017 IGN
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

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.DataFrame

package object xyz {

  /**
   * Adds a method, `xyz`, to DataFrameWriter that allows you to write xyz files using
   * the DataFileWriter
   */
  implicit class XyzDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def xyz: String => Unit = writer.format("fr.ign.spark.iqmulus.xyz").save
  }

  /**
   * Adds a method, `xyz`, to DataFrameReader that allows you to read xyz files using
   * the DataFileReader
   */
  implicit class XyzDataFrameReader(reader: DataFrameReader) {
    def xyz: String => DataFrame = reader.format("fr.ign.spark.iqmulus.xyz").load
  }

}
