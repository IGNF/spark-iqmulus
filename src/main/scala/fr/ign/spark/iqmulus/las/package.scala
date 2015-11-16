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
  
  implicit class LasDataFrame(df: DataFrame) {
     def saveAsLas(
         location: String, 
         formatOpt : Option[Byte] = None, 
         major  : Byte = 1, 
         minor  : Byte = 2, 
         scale  : Array[Double] = Array(0.01,0.01,0.01), 
         offset : Array[Double] = Array(0,0,0)
       ) = {
       val fieldSet = df.drop("gid").schema.fields.toSet
       val format = formatOpt.getOrElse((LasHeader.schema.indexWhere {schema => fieldSet subsetOf schema.fields.toSet }).toByte)
       if(format == -1) {
         sys.error(s"dataframe schema is not a subset of any LAS format schema")
       }
       val schema = LasHeader.schema(format) // no user types for now
       val cols   = schema.fieldNames.intersect(df.schema.fieldNames)
       //val conf = df.sqlContext.sparkContext.hadoopConfiguration // issue : not serializable
       df.select(cols.head, cols.tail :_*).rdd.mapPartitionsWithIndex({ // emulate foreachPartitionsWithIndex
         case (pid, iter) =>
           val rows = iter.toArray // materialize the partition to access it in a single pass, TODO workaround that 
           var count = rows.length
           val pmin = Array.fill[Double](3)(Double.PositiveInfinity)
           val pmax = Array.fill[Double](3)(Double.NegativeInfinity)
           val countByReturn = Array.fill[Long](15)(0)
           rows.foreach { row =>
             val x = offset(0)+scale(0)*row.getAs[Int]("x").toDouble
             val y = offset(1)+scale(1)*row.getAs[Int]("y").toDouble
             val z = offset(2)+scale(2)*row.getAs[Int]("z").toDouble
             val ret = row.getAs[Byte]("flags") & 0x3
             countByReturn(ret) += 1
             pmin(0) = Math.min(pmin(0),x)
             pmin(1) = Math.min(pmin(1),y)
             pmin(2) = Math.min(pmin(2),z)
             pmax(0) = Math.max(pmax(0),x)
             pmax(1) = Math.max(pmax(1),y)
             pmax(2) = Math.max(pmax(2),z)
           }
           val name = s"$location/$pid.las"
           val path = new org.apache.hadoop.fs.Path(name)
           val fs = path.getFileSystem(new org.apache.hadoop.conf.Configuration)
           val f = fs.create(path)
           val header = new LasHeader(name,format,count,pmin,pmax,scale,offset,version=Array(major,minor),pdr_return_nb=countByReturn)
           val dos = new java.io.DataOutputStream(f);
           header.write(dos)
           val ros = new RowOutputStream(dos,schema)
           rows.foreach(ros.write)
           dos.close
           Iterator((name,count))
       }, true)
     }
   }

}
