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

package fr.ign.spark.iqmulus.xyz

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.HadoopFsRelation
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileStatus

object XyzRelation {

  val xyzrgbSchema = StructType(Array(
        StructField("x",FloatType,false),
        StructField("y",FloatType,false),
        StructField("z",FloatType,false),
        StructField("r",ByteType,false),
        StructField("g",ByteType,false),
        StructField("b",ByteType,false)
  ))
  
  val xyzSchema = StructType(Array(
        StructField("x",FloatType,false),
        StructField("y",FloatType,false),
        StructField("z",FloatType,false)
  ))
  
}


class XyzRelation (override val paths: Array[String], val dataSchemaOpt : Option[StructType])
		(@transient val sqlContext: SQLContext)
		extends HadoopFsRelation with org.apache.spark.Logging {
  
  override lazy val dataSchema = dataSchemaOpt.getOrElse(XyzRelation.xyzrgbSchema)
    
	override def prepareJobForWrite(job: org.apache.hadoop.mapreduce.Job): org.apache.spark.sql.sources.OutputWriterFactory = ???

	override def buildScan(inputs: Array[FileStatus]): RDD[Row] = {
	  val lines = sqlContext.sparkContext.textFile(inputs.map(_.getPath).mkString("",",",""))
	  val dataTypes = dataSchema.fields.map(_.dataType)
	  lines map (line => Row.fromSeq((line.split("\t") take (dataTypes.size) zip dataTypes).map{
	    case (x,StringType)  => x
	    case (x,ByteType)    => x.toByte
	    case (x,ShortType)   => x.toShort
	    case (x,IntegerType) => x.toInt
	    case (x,LongType)    => x.toLong
	    case (x,FloatType)   => x.toFloat
	    case (x,DoubleType)  => x.toDouble
	    case _               => null
	  }.padTo(dataTypes.size,null)))
	}
	
}

