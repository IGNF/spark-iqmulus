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
  
}

