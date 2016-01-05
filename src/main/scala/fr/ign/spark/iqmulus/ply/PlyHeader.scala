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

package fr.ign.spark.iqmulus.ply

import fr.ign.spark.iqmulus.StructFieldWithGet
import java.nio.{ ByteBuffer, ByteOrder }
import java.io.{ BufferedReader, InputStreamReader, PushbackReader }
import org.apache.commons.io.input.CountingInputStream
import org.apache.hadoop.io.{ LongWritable, BytesWritable }
import org.apache.spark.sql.types._
import _root_.fr.ign.spark.iqmulus.BinarySection
import java.io.{ InputStream, DataOutputStream, FileInputStream }

case class PlyProperty(name: String, typename: String) {
  val dataType: DataType = typename match {
    case "uchar" => ByteType case "uint8" => ByteType // UnsignedByteType ???
    case "char" => ByteType case "int8" => ByteType
    case "ushort" => ShortType case "uint16" => ShortType // UnsignedShortType ???
    case "short" => ShortType case "int16" => ShortType
    case "uint" => IntegerType case "uint32" => IntegerType // UnsignedIntegerType ???
    case "int" => IntegerType case "int32" => IntegerType
    case "ulong" => LongType case "uint64" => LongType // UnsignedLongType ???
    case "long" => LongType case "int64" => LongType
    case "float" => FloatType case "float32" => FloatType
    case "double" => DoubleType case "float64" => DoubleType
    case other => sys.error(s"Unsupported type $other")
  }
  def this(name: String, dataType: DataType) {
    this(name, dataType match {
      case ByteType => "uchar"
      case ShortType => "int16"
      case IntegerType => "int32"
      case LongType => "int64"
      case FloatType => "float32"
      case DoubleType => "float64"
      case other => sys.error(s"Unsupported type $other")
    })
  }

  def size = dataType.defaultSize

  override def toString = "property " + typename + " " + name + "\n"

  def toStructField = StructField(name, dataType, nullable = false)
}

case class PlyElement(
    name: String,
    littleEndian: Boolean,
    count: Long,
    var properties: Seq[PlyProperty] = Seq[PlyProperty]()
) {

  def schema = StructType(properties.map(_.toStructField))

  def toBinarySection(location: String, offset: Long) =
    BinarySection(location, offset, count, littleEndian, schema)

  override def toString = "element " + name + " " + count + "\n" + properties.mkString

  def merge(that: PlyElement) = {
    require(this.properties sameElements that.properties)
    require(this.name == that.name)
    require(this.littleEndian == that.littleEndian)
    copy(count = this.count + that.count)
  }
}

case class PlyHeader(
    location: String,
    littleEndian: Boolean,
    length: Long,
    elements: Seq[PlyElement] = Seq.empty[PlyElement],
    obj_info: Seq[String] = Seq.empty[String],
    comments: Seq[String] = Seq.empty[String]
) {

  def write(dos: DataOutputStream) = dos writeBytes toString

  override def toString =
    "ply\n" +
      "format binary_" + (if (littleEndian) "little" else "big") +
      "_endian 1.0\n" +
      comments.map(_ + "\n").mkString +
      obj_info.map("obj_info " + _ + "\n").mkString +
      elements.mkString +
      "end_header\n"

  lazy val section = {
    val sections = elements.map(_.toBinarySection(location, 0))
    val offsets: Seq[Long] = sections.map(_.size).toSeq.scanLeft(length)(_ + _)
    (sections zip offsets) foreach (x => x._1.offset = x._2)
    (sections zip elements).map(x => (x._2.name, x._1)).toMap
  }

  def this(
    location: String,
    littleEndian: Boolean,
    schemas: Map[String, (Long, StructType)],
    obj_info: Seq[String],
    comments: Seq[String]
  ) {
    this(location, littleEndian, 0, schemas.map {
      case (name, (count, schema)) =>
        PlyElement(name, littleEndian, count, schema.fields.map { f =>
          new PlyProperty(f.name, f.dataType)
        })
    }.toSeq, obj_info, comments)
  }

  def this(
    location: String,
    littleEndian: Boolean,
    schemas: Map[String, (Long, StructType)]
  ) {
    this(location, littleEndian, schemas, Seq.empty[String], Seq.empty[String])
  }

  def merge(that: PlyHeader) = {
    require(this.littleEndian == that.littleEndian)
    require(this.elements.length == that.elements.length)
    val zippedElements = this.elements.zip(that.elements)
    require(zippedElements.forall { case (x, y) => x.name == y.name })
    new PlyHeader(
      "",
      littleEndian,
      0,
      zippedElements.map { case (x, y) => x merge y },
      this.obj_info ++ that.obj_info,
      this.comments ++ that.comments
    )
  }
}

object PlyHeader {
  def read(location: String): PlyHeader =
    read(location, new FileInputStream(location))

  def read(path: org.apache.hadoop.fs.Path): PlyHeader =
    read(org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(path).toString)

  def read(location: String, in: java.io.InputStream): PlyHeader = {
    val pb = new PushbackReader(new InputStreamReader(in), 5)
    val br = new BufferedReader(pb)
    try {
      var littleEndian = false
      var comments = Seq.empty[String]
      var obj_info = Seq.empty[String]
      var elements = Seq.empty[PlyElement]
      val line1 = "ply\r\n".toArray
      var nread = pb.read(line1, 0, 5)
      val nl = if (nread == 5 && "\r\n".contains(line1(4))) 2 else 1
      pb.unread(line1)
      var line = br.readLine
      var offset = line.length + nl
      if (line != "ply") {
        throw new java.lang.IllegalArgumentException(s"Not a PLY file (starts with ${line.slice(0, 4)})")
      }
      line = br.readLine
      while (line != null) {
        offset += line.length + nl
        line.split("\\s+").toSeq match {
          case Nil =>
          case "format" +: "binary_little_endian" +: "1.0" +: Nil => littleEndian = true
          case "format" +: "binary_big_endian" +: "1.0" +: Nil => littleEndian = false
          case "format" +: _ => throw new java.lang.IllegalArgumentException(s"PLY file with unsupported format ($line)")
          case "comment" +: _ => comments :+= line;
          case "obj_info" +: _ => obj_info :+= line;
          case "element" +: name +: count +: Nil => elements :+= PlyElement(name, littleEndian, count.toLong)
          case "property" +: typename +: name +: Nil => elements.last.properties :+= PlyProperty(name, typename)
          case "end_header" +: Nil =>
            return PlyHeader(location, littleEndian, offset, elements, obj_info, comments)

          case _ => throw new java.lang.IllegalArgumentException(s"Ill-formed PLY header line : $line")
        }
        line = br.readLine
      }
      throw new java.lang.IllegalArgumentException(s"Truncated PLY header")
    } finally {
      br.close
    }
  }
}

