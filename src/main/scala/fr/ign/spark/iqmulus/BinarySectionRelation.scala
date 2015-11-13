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

import java.nio.{ByteBuffer, ByteOrder}
import org.apache.hadoop.io._
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.sources.HadoopFsRelation
import org.apache.spark.sql.types._
import org.apache.spark.rdd.{ NewHadoopPartition, NewHadoopRDD, RDD, UnionRDD }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.{ FileSystem, FileStatus, Path }

case class BinarySection(
		location: String,
		var offset: Long,
		count: Long,
		littleEndian: Boolean,
		private val _schema: StructType,
		private val _stride: Int = 0,
		gidName: String = "gid") {

	val sizes = _schema.fields.map(_.dataType.defaultSize)
	val offsets = sizes.scanLeft(0)(_ + _)
	val length = offsets.last
	val stride = if (_stride > 0) _stride else length
	def size: Long = stride * count
	val schema = StructType(StructField(gidName, LongType, false) +: _schema.fields)
      
    val fieldOffsetMap : Map[String,(StructField,Int)]= (schema.fields.tail zip offsets) map (x => (x._1.name,x)) toMap

	def order = if (littleEndian) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN
	def toBuffer(bytes: BytesWritable): ByteBuffer = ByteBuffer.wrap(bytes.getBytes).order(order)

	def bytesSeq(prop: Seq[(StructField, Int)]): Seq[ByteBuffer => Any] = prop map { case (p, o) => p.get(o) }

	def getSeqAux(prop: Seq[(StructField, Int)], gid: Boolean = true)(id: LongWritable, bytes: BytesWritable): Seq[Any] = {
		val seq = (bytesSeq(prop) map (_(toBuffer(bytes))))
		if (gid) (id.get +: seq) else seq
	}

	def getSubSeq(dataSchema: StructType, requiredColumns: Array[String]) = {
  	  val fieldsWithOffsets = dataSchema.fields.tail.map(field => fieldOffsetMap.getOrElse(field.name,(StructField(field.name,NullType,nullable = true),0)))
      val requiredFieldsWithOffsets = fieldsWithOffsets filter (requiredColumns contains _._1.name)
      getSeqAux(requiredFieldsWithOffsets, requiredColumns contains gidName) _
	}
}

/**
 * TODO
 * @param TODO
 */
abstract class BinarySectionRelation extends HadoopFsRelation {

	def sections: Array[BinarySection]

	/**
	 * Determine the RDD Schema based on the se header info.
	 * @return StructType instance
	 */
	//override val dataSchema: StructType = sections.head.schema
	override val dataSchema: StructType = sections.map(_.schema).reduce(_ merge _)

	override def paths = sections.map(_.location)

	override def prepareJobForWrite(job: org.apache.hadoop.mapreduce.Job): org.apache.spark.sql.sources.OutputWriterFactory = ???


	private[iqmulus] def baseRDD(section : BinarySection, toSeq: ((LongWritable, BytesWritable) => Seq[Any])): RDD[Row] = {
		val conf = sqlContext.sparkContext.hadoopConfiguration
				conf.set(FixedLengthBinarySectionInputFormat.RECORD_OFFSET_PROPERTY, section.offset.toString)
				conf.set(FixedLengthBinarySectionInputFormat.RECORD_COUNT_PROPERTY, section.count.toString)
				conf.set(FixedLengthBinarySectionInputFormat.RECORD_STRIDE_PROPERTY, section.stride.toString)
				conf.set(FixedLengthBinarySectionInputFormat.RECORD_LENGTH_PROPERTY, section.length.toString)
				val rdd = sqlContext.sparkContext.newAPIHadoopFile(section.location,
						classOf[FixedLengthBinarySectionInputFormat],
						classOf[LongWritable],
						classOf[BytesWritable])
              rdd.map({ case (id, bytes) => Row.fromSeq(toSeq(id, bytes)) })
						//    rdd.map(Row.fromSeq(toRowSeq(_)))
	}

	//def buildScan(columns: Array[String]): RDD[Row] = baseRDD(section.getSubSeq(columns))
	//def buildScan(): RDD[Row] = baseRDD(section.getSeq)

	override def buildScan(requiredColumns: Array[String], inputs: Array[FileStatus]): RDD[Row] = {
		if (inputs.isEmpty) {
			sqlContext.sparkContext.emptyRDD[Row]
		} else {
			new UnionRDD[Row](sqlContext.sparkContext,
					sections.map {section => baseRDD(section,section.getSubSeq(dataSchema,requiredColumns)) })
		}
	}

}



