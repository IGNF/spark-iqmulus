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

import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers

class LasSuite extends FunSuite with ShouldMatchers {

  val files = Seq(
    ("ComplexSRSInfo.las"              ,1,0,0,5,20,11781),
    ("las14_type7_ALS_RIEGL_Q680i.las" ,1,4,7,3,40,94451),
    ("las14_type8_vahingen_OPALS.las"  ,1,4,8,1,40,451351),
    ("las14_type6_ALS_RIEGL_VQ580.las" ,1,4,6,3,36,26202),
    ("las14_type7_fugro_sample.las"    ,1,4,7,0,36,20),
    ("las14_type6_MLS_RIEGL_VMX450.las",1,4,6,3,36,99756),
    ("las14_type7_globalmapper.las"    ,1,4,7,2,36,22600),
    ("libLAS_1.2.las"                  ,1,2,0,0,20,497536),
    ("lt_srs_rt.las"                   ,1,0,0,5,20,11781),
    ("lt_srs_rt-qt-1.1.las"            ,1,1,0,0,20,11781),
    ("lt_srs_rt-qt-1.2.las"            ,1,1,0,0,20,11781),
    ("srs.las"                         ,1,0,1,3,28,10)
  )
  
  val resources = "src/test/resources"

  files foreach { case (file,major,minor,pdr_format,vlr_nb,pdr_length,pdf_nb) =>
    if(new java.io.File(s"$resources/$file").exists) {
      test(s"LAS$major.$minor/$pdr_format: $file should read the correct header metadata") {
        val header = LasHeader.read(s"$resources/$file").get;
        header.version should equal(Array(major.toByte, minor.toByte))
        header.pdr_format should be(pdr_format)
        header.vlr_nb should be(vlr_nb)
        header.pdr_length should be(pdr_length)
        header.pdr_nb should be (pdf_nb)
        header.pdr_return_nb.sum should ( be (header.pdr_nb) or be (0) )
      }
    }
  }
}
