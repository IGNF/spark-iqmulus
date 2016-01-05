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

import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import org.apache.spark.sql.types._

class PlySuite extends FunSuite with ShouldMatchers {

  val id = Array("fid" -> IntegerType, "pid" -> LongType)
  val xyz = Array("x" -> FloatType, "y" -> FloatType, "z" -> FloatType)
  val rgb = Array("r" -> ByteType, "g" -> ByteType, "b" -> ByteType)

  val files = Seq(
    ("trepied_xyz.ply", 5995, id ++ xyz) // ,
  //   ("trepied_dim.ply", 5995, id ++ xyz ++ rgb),
  //   ("trepied_dim2.ply", 5995, id ++ xyz ++ rgb),
  //   ("213-232-7.ply", 71651, id ++ xyz ++ rgb)
  )

  val resources = "src/test/resources"

  files foreach {
    case (file, count, fields) =>
      if (new java.io.File(s"$resources/$file").exists) {
        test(s"$file should read the correct header metadata") {
          val header = PlyHeader.read(s"$resources/$file");
          header.section("vertex").count should equal(count)
        }

        test(s"$file should have the correct schema") {
          val header = PlyHeader.read(s"$resources/$file");
          header.section("vertex").schema should equal(StructType(fields map {
            case (name, dataType) => StructField(name, dataType, nullable = false)
          }))
        }
      }
  }
}
