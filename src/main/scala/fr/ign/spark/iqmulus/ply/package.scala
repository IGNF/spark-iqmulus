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

import org.apache.spark.sql.{SQLContext, DataFrameReader, DataFrameWriter, DataFrame, Row}
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
     def saveAsPly(location: String, littleEndian : Boolean = true) = {
       val df2 = df.drop("id")//.na.fill(0)
       val schema = df2.schema
       val saver = (key: Int,iter:Iterator[Row]) => Iterator(savePlyRow(schema,littleEndian,filename(location) _)(key,iter))
       df2.rdd.mapPartitionsWithIndex(saver, true).collect
     }
   }

   def filename(location : String)(key : Int) = s"$location/$key.ply"
   
   def savePlyProduct[Key](
       schema : StructType,
       littleEndian : Boolean,
       filename : (Key => String) )
     (key : Key, iter : Iterator[Product]) = {
     savePlyRow(schema,littleEndian,filename)(key,iter.map(Row.fromTuple))
   }

   def savePly[Key,Value](
       schema : StructType,
       littleEndian : Boolean,
       filename : (Key => String) )
     (key : Key, iter : Iterator[Value]) = {
     if(iter.isInstanceOf[Iterator[Product]])
       savePlyProduct(schema,littleEndian,filename)(key,iter.asInstanceOf[Iterator[Product]])
     else savePlyRow(schema,littleEndian,filename)(key,iter.map(value => Row.apply(value)))
   }
   
   def savePlyRow[Key](
       schema : StructType,
       littleEndian : Boolean,
       filename : (Key => String) )
     (key : Key, iter : Iterator[Row]) = {
      val name = filename(key)
      val path = new org.apache.hadoop.fs.Path(name)
      val fs = path.getFileSystem(new org.apache.hadoop.conf.Configuration)
      val f = fs.create(path)
      val rows = iter.toArray
      val count = rows.size.toLong
      val header = new PlyHeader(Map("vertex" -> ((count, schema))), littleEndian)
      val dos = new java.io.DataOutputStream(f);
      dos.write(header.toString.getBytes)
      val ros = new RowOutputStream(dos,littleEndian,schema)
      rows.foreach(ros.write)
      dos.close
      (name,count)
    }
   
}

