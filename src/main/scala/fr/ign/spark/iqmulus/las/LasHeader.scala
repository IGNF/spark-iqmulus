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
import fr.ign.spark.iqmulus.BinarySection
import java.nio.{ ByteBuffer, ByteOrder }
import java.io.{ InputStream, DataOutputStream, FileInputStream }

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

  def schema: StructType = LasHeader.schema(pdr_format)
  def header_size: Short = LasHeader.header_size(version.major)(version.minor)
  def pdr_offset: Int = if (pdr_offset0 > 0) pdr_offset0 else header_size
  def pdr_length: Short = Math.max(pdr_length_header, LasHeader.pdr_length(pdr_format)).toShort

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
  Number of dimensions:        ${schema.fields.length}%d
  Custom schema?:              false
  Size in bytes:               $pdr_length%d
"""

  /*
// scalastyle:off
f"""
---------------------------------------------------------
  Dimensions
---------------------------------------------------------
  'X'                            --  size: 32 offset: 0
  'Y'                            --  size: 32 offset: 4
  'Z'                            --  size: 32 offset: 8
  'Intensity'                    --  size: 16 offset: 12
  'Return Number'                --  size: 3 offset: 14
  'Number of Returns'            --  size: 3 offset: 14
  'Scan Direction'               --  size: 1 offset: 14
  'Flightline Edge'              --  size: 1 offset: 14
  'Classification'               --  size: 8 offset: 15
  'Scan Angle Rank'              --  size: 8 offset: 16
  'User Data'                    --  size: 8 offset: 17
  'Point Source ID'              --  size: 16 offset: 18
  
---------------------------------------------------------
  Point Inspection Summary
---------------------------------------------------------
  Header Point Count: 497536
  Actual Point Count: 497536

  Minimum and Maximum Attributes (min,max)
---------------------------------------------------------
  Min X, Y, Z:    1440000.00, 375000.03, 832.18
  Max X, Y, Z:    1444999.96, 379999.99, 972.67
  Bounding Box:   1440000.00, 375000.03, 1444999.96, 379999.99
  Time:     0.000000, 0.000000
  Return Number:  0, 0
  Return Count:   0, 0
  Flightline Edge:  0, 0
  Intensity:    0, 255
  Scan Direction Flag:  0, 0
  Scan Angle Rank:  0, 0
  Classification: 1, 5
  Point Source Id:  29, 30
  User Data:    0, 0
  Minimum Color (RGB):  0 0 0 
  Maximum Color (RGB):  0 0 0 

  Number of Points by Return
---------------------------------------------------------
  (1) 497536

  Number of Returns by Pulse
---------------------------------------------------------
  (0) 497536

  Point Classifications
---------------------------------------------------------
  19675 Unclassified (1) 
  402812 Ground (2) 
  75049 High Vegetation (5) 
  -------------------------------------------------------
    0 withheld
    0 keypoint
    0 synthetic
  -------------------------------------------------------
"""
// scalastyle:on
*/

  def toBinarySection: BinarySection = {
    BinarySection(location, pdr_offset, pdr_nb, true, schema, pdr_length)
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

/*
    // not available in Las 1.0
    val wdpr_offset = buffer.getLong(227)
    val evlr_offset = buffer.getLong(235)
    val evlr_nb = buffer.getInt(243)
    val pdr_nb = buffer.getLong(247)
    val pdr_return_nb = get(255, 15, 8, buffer.getLong)
    if (version.major > 1 || version.minor > 0) {
      println(s"wdpr_offset = $wdpr_offset")
      println(s"evlr_offset = $evlr_offset")
      println(s"evlr_nb = $evlr_nb")
      println(s"pdr_nb = $pdr_nb")
      println(s"pdr_return_nb = ${pdr_return_nb.mkString(",")}")
    }
    */

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
      "x" -> IntegerType,
      "y" -> IntegerType,
      "z" -> IntegerType,
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
      val fieldSet = schema.fields.toSet - "id"
      def subSchema(schema: StructType) = fieldSet subsetOf schema.fields.toSet
      val format = (LasHeader.schema indexWhere subSchema).toByte
      if (format == -1) {
        sys.error(s"dataframe schema is not a subset of any LAS format schema")
      }
      format
    }

  def read(location: String): Option[LasHeader] =
    read(location, new FileInputStream(location))

  def read(location: String, in: InputStream): Option[LasHeader] = {
    val dis = new java.io.DataInputStream(in)
    val bytes = Array.ofDim[Byte](375)
    try dis.readFully(bytes)
    catch {
      case e: java.io.IOException =>
        println(s"$location : Unable to read (${bytes.length} bytes, skipping.");
        return None
    }
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
      println(s"$location : not a LAS file, skipping (signature=$signature)");
      return None
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

    Some(LasHeader(
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
    ))
  }
}
