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

package fr.ign.spark.iqmulus.las

import scala.reflect.ClassTag
import org.apache.hadoop.io._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.OffsetScaledIntegerType
import fr.ign.spark.iqmulus.BinarySection
import java.nio.{ ByteBuffer, ByteOrder }
import java.io.{ InputStream, DataOutputStream, FileInputStream, DataInputStream, BufferedInputStream }

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.deploy.SparkHadoopUtil

case class Version(
    major: Byte = Version.majorDefault,
    minor: Byte = Version.minorDefault
) {
  override def toString = s"$major.$minor"
}

object Version {
  val majorDefault: Byte = 1
  val minorDefault: Byte = 2
  def fromString(version: String) = {
    val Array(major, minor) = version.split('.') map (_.toByte)
    Version(major, minor)
  }
}

case class VariableLengthRecord(bytes: Array[Byte], offset: Long, extended: Boolean) {
  val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
  def reserved = bytes.slice(0, 2)
  def userIDBytes = bytes.slice(2, 18)
  def recordID = buffer.getShort(18)
  val recordLength = if (extended) buffer.getLong(20) else buffer.getShort(20)
  def descriptionBytes = if (extended) bytes.slice(28, 60) else bytes.slice(22, 56)

  def userID = new String(userIDBytes takeWhile (_ != 0) map (_.toChar))
  def description = new String(descriptionBytes takeWhile (_ != 0) map (_.toChar))

  override def toString = s"userID     =$userID\nrecordID   =$recordID\ndescription=$description\n"
}

import java.nio.{ ByteBuffer, ByteOrder }
import org.apache.spark.sql.types._

class ExtraBytes(bytes: Array[Byte]) {

  def this(length: Byte) = this(ExtraBytes.defaultBytes.patch(3, Array(length), 1))

  private def getAnyArray(offset: Int) = upcastDataType match {
    case LongType => getLongArray(offset)
    case DoubleType => getDoubleArray(offset)
  }
  private def getLongArray(offset: Int) = (for (i <- 0 until dim) yield buffer.getLong(offset + 8 * i)).toArray
  private def getDoubleArray(offset: Int) = (for (i <- 0 until dim) yield buffer.getDouble(offset + 8 * i)).toArray

  private val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

  def reserved = bytes.slice(0, 2)
  def data_type = bytes(2)
  def options = bytes(3)
  def nameBytes = bytes.slice(4, 36)
  def unused = bytes.slice(36, 40)
  def no_data = getAnyArray(40)
  def min = getAnyArray(64)
  def max = getAnyArray(88)
  def scale = getDoubleArray(112)
  def offset = getDoubleArray(136)
  def descBytes = bytes.slice(160, 192)

  def dim = if (data_type == 0) options else (((data_type - 1) / 10) + 1)
  def typ = if (data_type == 0) 0 else (((data_type - 1) % 10) + 1)
  def dataType = ExtraBytes.dataType(typ)
  def upcastDataType = ExtraBytes.upcastDataType(typ)

  def name = new String(nameBytes takeWhile (_ != 0) map (_.toChar))
  def description = new String(descBytes takeWhile (_ != 0) map (_.toChar))

  def schema(fid: Int) = StructType(Array.tabulate(dim)(i => {
    val fieldName = if (dim == 1) name else s"$name$i"
    val metadata = new MetadataBuilder()
    val opt = (0 until 5) map (j => (options & (1 << j)) != 0)
    upcastDataType match {
      case LongType =>
        if (opt(0)) metadata.putLong("nodata", getLongArray(40)(i))
        if (opt(1)) metadata.putLong("min", getLongArray(64)(i))
        if (opt(2)) metadata.putLong("max", getLongArray(88)(i))
      case DoubleType =>
        if (opt(0)) metadata.putDouble("nodata", getDoubleArray(40)(i))
        if (opt(1)) metadata.putDouble("min", getDoubleArray(64)(i))
        if (opt(2)) metadata.putDouble("max", getDoubleArray(88)(i))
    }
    if (opt(3)) metadata.putDouble("scale", scale(i))
    if (opt(4)) metadata.putDouble("offset", offset(i))
    val builder = new MetadataBuilder()
    builder.putMetadata(fid.toString, metadata.build)
    StructField(fieldName, dataType, opt(0), builder.build)
  }))
}

object ExtraBytes {
  val defaultBytes = {
    val bytes = Array.ofDim[Byte](192)
    "ExtraByte".zipWithIndex.foreach { case (c, i) => bytes(4 + i) = c.toByte }
    "Undefined extra byte".zipWithIndex.foreach { case (c, i) => bytes(160 + i) = c.toByte }
    bytes
  }
  val userIDBytes = "LASF_Spec".getBytes.padTo(16, 0.toByte)
  val recordID = 4.toShort
  val dataType = Array(
    ByteType,
    ByteType, ByteType,
    ShortType, ShortType,
    IntegerType, IntegerType,
    LongType, LongType,
    FloatType, DoubleType
  )

  val upcastDataType = Array(
    ByteType,
    LongType, LongType,
    LongType, LongType,
    LongType, LongType,
    LongType, LongType,
    DoubleType, DoubleType
  )
}

case class ProjectID(
    ID1: Int = 0,
    ID2: Short = 0,
    ID3: Short = 0,
    ID4: Array[Byte] = Array.fill[Byte](8)(0)
) {
  override def toString =
    f"${ID4.mkString}%s-0000-$ID3%04d-$ID2%04d-$ID1%08d"
}

case class LasHeader(
    location: String,
    pdr_format: Byte,
    pdr_nb: Long = 0,
    pmin: Array[Double] = Array.fill[Double](3)(0),
    pmax: Array[Double] = Array.fill[Double](3)(0),
    scale: Array[Double] = Array.fill[Double](3)(1),
    offset: Array[Double] = Array.fill[Double](3)(0),
    pdr_return_nb: Array[Long] = Array.fill[Long](15)(0),
    pdr_offset0: Int = 0,
    systemID: String = "spark",
    software: String = "fr.ign.spark.iqmulus",
    version: Version = Version(),
    sourceID: Short = 0,
    globalEncoding: Short = 0,
    vlr_nb: Int = 0,
    pdr_length_header: Short = 0,
    projectID: ProjectID = ProjectID(),
    creation: Array[Short] = Array[Short](0, 0),
    waveform_offset: Long = 0,
    evlr_offset: Long = 0,
    evlr_nb: Int = 0
) {

  def extraBytesSchema(fid: Int) = StructType(extraBytes.map(_.schema(fid).fields).fold(Array.empty)(_ ++ _))
  def customSchema = pdr_length != LasHeader.pdr_length(pdr_format)

  def extraBytes: Array[ExtraBytes] = {
    if (!customSchema) return Array.empty[ExtraBytes]

    val path = new Path(location)
    val fs = path.getFileSystem(SparkHadoopUtil.get.conf)
    val fileInputStream = fs.open(path)
    val vlrWithBytes = try {
      for (
        r <- vlr if r.recordID == ExtraBytes.recordID && (r.userIDBytes sameElements ExtraBytes.userIDBytes)
      ) yield {
        fileInputStream.seek(r.offset)
        val bytes = Array.ofDim[Byte](r.recordLength.toInt)
        fileInputStream.readFully(bytes)
        (r, bytes)
      }
    } finally {
      fileInputStream.close
    }
    val res = for ((r, bytes) <- vlrWithBytes; i <- 0 until r.recordLength.toInt by 192)
      yield new ExtraBytes(bytes.slice(i, i + 192))

    if (res.isEmpty) {
      Array(new ExtraBytes(Math.min(255, pdr_length - LasHeader.pdr_length(pdr_format)).toByte))
    } else {
      res.toArray
    }
  }

  lazy val vlr = getVLR(false) ++ getVLR(true)

  def getVLR(extended: Boolean) = {
    val path = new Path(location)
    val fs = path.getFileSystem(SparkHadoopUtil.get.conf)
    val fileInputStream = fs.open(path)

    try {
      val nb = if (extended) evlr_nb else vlr_nb
      val header = if (extended) 60 else 54
      var offset = if (extended) evlr_offset.toLong else header_size.toLong
      for (i <- 1 to nb) yield {
        fileInputStream.seek(offset)
        val bytes = Array.ofDim[Byte](header)
        fileInputStream.readFully(bytes)
        offset += header
        val vlr = VariableLengthRecord(bytes, offset, false)
        offset += vlr.recordLength
        vlr
      }
    } finally {
      fileInputStream.close
      fs.close
    }
  }

  // todo: add metadata to the schema for non-extra-byte columns
  def schema(fid: Int): StructType = StructType(LasHeader.schema(pdr_format) ++ extraBytesSchema(fid))
  def header_size: Short = LasHeader.header_size(version.major)(version.minor)
  def pdr_offset: Int = if (pdr_offset0 > 0) pdr_offset0 else header_size
  def pdr_length: Short = Math.max(pdr_length_header, LasHeader.pdr_length(pdr_format)).toShort

  // lasinfo style printing
  // scalastyle:off
  override def toString = f"""
---------------------------------------------------------
Header Summary
---------------------------------------------------------

Version:                     $version%s
Source ID:                   $sourceID%s
Reserved:                    $globalEncoding%s
Project ID/GUID:             '$projectID%s'
System ID:                   '$systemID%s'
Generating Software:         '$software%s'
File Creation Day/Year:      ${creation.mkString("/")}%s
Header Byte Size             $header_size%d
Data Offset:                 $pdr_offset%d
Header Padding:              0
Number Var. Length Records:  ${if (vlr_nb > 0) vlr_nb else "None"}%s
Point Data Format:           $pdr_format%d
Number of Point Records:     $pdr_nb%d
Compressed:                  False
Number of Points by Return:  ${pdr_return_nb.mkString(" ")}%s
Scale Factor X Y Z:          ${scale.mkString(" ")}%s
Offset X Y Z:                ${offset.mkString(" ")}%s
Min X Y Z:                   ${pmin(0)}%.2f ${pmin(1)}%.2f ${pmin(2)}%f 
Max X Y Z:                   ${pmax(0)}%.2f ${pmax(1)}%.2f ${pmax(2)}%f
Spatial Reference:           None

---------------------------------------------------------
Schema Summary
---------------------------------------------------------
Point Format ID:             $pdr_format%d
Number of dimensions:        ${schema(0).fields.length}%d
Custom schema?:              ${if (customSchema) "true" else "false"}
Size in bytes:               $pdr_length%d

---------------------------------------------------------
  Dimensions
---------------------------------------------------------
${schema(0).map(f => s"  '${f.name}'".padTo(34, ' ') + s"--  size : ${f.dataType.defaultSize}").mkString("\n")}
"""
  // scalastyle:on

  def toBinarySection(paths: Array[String]): BinarySection = {
    BinarySection(location, pdr_offset, pdr_nb, true, schema(paths.indexOf(location)), pdr_length)
  }

  def write(dos: DataOutputStream): Unit = {
    val bytes = Array.fill[Byte](header_size)(0);
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    def legacy(x: Long) = if (x > Int.MaxValue) 0 else x.toInt

    buffer.put("LASF".getBytes)
    buffer.putShort(sourceID)
    buffer.putShort(globalEncoding)
    buffer.putInt(projectID.ID1)
    buffer.putShort(projectID.ID2)
    buffer.putShort(projectID.ID3)
    projectID.ID4.take(8).foreach(buffer.put)
    buffer.put(version.major)
    buffer.put(version.minor)
    buffer.put(systemID.padTo(32, '\0').getBytes)
    buffer.put(software.padTo(32, '\0').getBytes)
    buffer.putShort(creation(0))
    buffer.putShort(creation(1))
    buffer.putShort(header_size)
    buffer.putInt(pdr_offset)
    buffer.putInt(vlr_nb)
    buffer.put(pdr_format)
    buffer.putShort(pdr_length)
    buffer.putInt(legacy(pdr_nb))
    pdr_return_nb.take(5).foreach { x => buffer.putInt(legacy(x)) }
    scale.take(3).foreach(buffer.putDouble)
    offset.take(3).foreach(buffer.putDouble)
    buffer.putDouble(pmax(0))
    buffer.putDouble(pmin(0))
    buffer.putDouble(pmax(1))
    buffer.putDouble(pmin(1))
    buffer.putDouble(pmax(2))
    buffer.putDouble(pmin(2))
    if (version.minor >= 3) {
      buffer.putLong(waveform_offset)
    }
    if (version.minor >= 4) {
      buffer.putLong(evlr_offset)
      buffer.putInt(evlr_nb)
      buffer.putLong(pdr_nb)
      pdr_return_nb.take(15).foreach(buffer.putLong)
    }
    dos.write(bytes)
  }

}

object LasHeader {
  val header_size: Map[Int, Array[Short]] = Map(1 -> Array(227, 227, 227, 235, 375))
  def pdr_length(format: Byte) = schema(format).defaultSize.toShort

  val schema: Array[StructType] = {
    val array = Array.ofDim[Array[(String, DataType)]](11)
    val color = Array(
      "red" -> ShortType,
      "green" -> ShortType,
      "blue" -> ShortType
    )

    val point = Array(
      "x" -> IntegerType, // OffsetScaledIntegerType(0, 0.01),
      "y" -> IntegerType, // OffsetScaledIntegerType(0, 0.01),
      "z" -> IntegerType, // OffsetScaledIntegerType(0, 0.01),
      "intensity" -> ShortType
    )

    val fw = Array(
      "index" -> ByteType,
      "offset" -> LongType,
      "size" -> IntegerType,
      "location" -> FloatType,
      "xt" -> FloatType,
      "yt" -> FloatType,
      "zt" -> FloatType
    )

    array(0) = point ++ Array(
      "flags" -> ByteType,
      "classification" -> ByteType,
      "angle" -> ByteType,
      "user" -> ByteType,
      "source" -> ShortType
    )

    array(6) = point ++ Array(
      "return" -> ByteType,
      "flags" -> ByteType,
      "classification" -> ByteType,
      "user" -> ByteType,
      "angle" -> ShortType,
      "source" -> ShortType,
      "time" -> DoubleType
    )

    array(1) = array(0) ++ Array("time" -> DoubleType)
    array(2) = array(0) ++ color
    array(3) = array(1) ++ color
    array(4) = array(1) ++ fw
    array(5) = array(3) ++ fw
    array(7) = array(6) ++ color
    array(8) = array(7) ++ Array("nir" -> ShortType)
    array(9) = array(6) ++ fw
    array(10) = array(8) ++ fw

    def toStructType(fields: Array[(String, DataType)]) =
      StructType(fields map (field => StructField(field._1, field._2, nullable = false)))
    array map toStructType
  }

  def formatFromSchema(schema: StructType): Byte =
    {
      val fieldSet = schema
        .map(_.copy(nullable = false))
        .filterNot(f => f.name == "pid" || f.name == "fid").toSet
      def subSchema(schema: StructType) = fieldSet subsetOf schema.fields.toSet
      val format = (LasHeader.schema indexWhere subSchema).toByte
      require(format >= 0, s"Dataframe schema is not a subset of any LAS format schema:\n${fieldSet mkString "\n"}")
      format
    }

  def read(location: String): LasHeader =
    read(location, new FileInputStream(location))

  def read(location: String, in: InputStream): LasHeader = {
    val dis = new java.io.DataInputStream(in)
    val bytes = Array.ofDim[Byte](375)
    dis.readFully(bytes)
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    def readString(offset: Int, length: Int) = {
      val buf = Array.ofDim[Byte](length);
      buffer.position(offset);
      buffer.get(buf);
      new String(buf takeWhile (_ != 0) map (_.toChar))
    }

    def get[A: ClassTag](index: Int, n: Int, stride: Int, f: Int => A) =
      Array.tabulate(n)((i: Int) => f(index + stride * i))

    val signature = readString(0, 4)
    if (signature != "LASF") {
      throw new java.lang.IllegalArgumentException(s"Not a LAS file (signature=$signature)")
    }

    val sourceID = buffer.getShort(4)
    val globalEncoding = buffer.getShort(6)
    val projectID = ProjectID(
      buffer.getInt(8),
      buffer.getShort(12),
      buffer.getShort(14),
      get(16, 8, 1, buffer.get)
    )
    val version = Version(buffer.get(24), buffer.get(25))
    val systemID = readString(26, 32)
    val software = readString(58, 32)
    val creation = get(90, 2, 2, buffer.getShort)
    val header_size = buffer.getShort(94)
    val pdr_offset = buffer.getInt(96)
    val vlr_nb = buffer.getInt(100)
    val pdr_format = buffer.get(104)
    val pdr_length = buffer.getShort(105)
    val pdr_nb_legacy = buffer.getInt(107)
    val pdr_return_nb_legacy = get(111, 5, 4, buffer.getInt)
    val scale = get(131, 3, 8, buffer.getDouble)
    val offset = get(155, 3, 8, buffer.getDouble)
    val pmin = get(187, 3, 16, buffer.getDouble)
    val pmax = get(179, 3, 16, buffer.getDouble)

    var waveform_offset: Long = 0
    var evlr_offset: Long = 0
    var evlr_nb = 0
    var pdr_nb = pdr_nb_legacy.toLong
    var pdr_return_nb = pdr_return_nb_legacy.map(_.toLong)

    if (version.minor >= 3) {
      waveform_offset = buffer.getLong(227)
    }
    if (version.minor >= 4) {
      evlr_offset = buffer.getLong(235)
      evlr_nb = buffer.getInt(243)
      pdr_nb = buffer.getLong(247)
      pdr_return_nb = get(255, 15, 8, buffer.getLong)
    }

    LasHeader(
      location,
      pdr_format,
      pdr_nb,
      pmin,
      pmax,
      scale,
      offset,
      pdr_return_nb,
      pdr_offset,
      systemID,
      software,
      version,
      sourceID,
      globalEncoding,
      vlr_nb,
      pdr_length,
      projectID,
      creation,
      waveform_offset,
      evlr_offset,
      evlr_nb
    )
  }
}
