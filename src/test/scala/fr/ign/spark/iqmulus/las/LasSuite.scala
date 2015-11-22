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
import sys.process._
import java.net.URL
import java.io.File
import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.FileOutputStream
import java.io.FileInputStream
import java.security.MessageDigest

class LasSuite extends FunSuite with ShouldMatchers {

  // (major,minor,pdr_format,vlr_nb,pdr_length,pdf_nb,sha1,url)
  val files = Seq(
    (1, 4, 7, 3, 40, 94451, "eaaf1ff16a79532d134be464f68324e5850ff8e8", "http://www.cs.unc.edu/~isenburg/lastools/las14/las14_type7_ALS_RIEGL_Q680i.las"),
    (1, 4, 8, 1, 40, 451351, "2bbdebe9e9cb7bee01eb5b077671d96b222d976a", "http://www.cs.unc.edu/~isenburg/lastools/las14/las14_type8_vahingen_OPALS.las"),
    (1, 4, 6, 3, 36, 26202, "5dcd5300a788a31c04efa593cac03f8feefe0b51", "http://www.cs.unc.edu/~isenburg/lastools/las14/las14_type6_ALS_RIEGL_VQ580.las"),
    (1, 4, 7, 0, 36, 20, "bf4736040f40156611f11e84bed9f78f473c8e92", "http://www.cs.unc.edu/~isenburg/lastools/las14/las14_type7_fugro_sample.las"),
    (1, 4, 6, 3, 36, 99756, "7c7fbf15187bd27c3af31f53185665a1d6480fe2", "http://www.cs.unc.edu/~isenburg/lastools/las14/las14_type6_MLS_RIEGL_VMX450.las"),
    (1, 4, 7, 2, 36, 22600, "ad26eb6919c89d9ddbd18012eed0765d03487a4f", "http://www.cs.unc.edu/~isenburg/lastools/las14/las14_type7_globalmapper.las"),
    (1, 0, 0, 5, 20, 11781, "66f9d75c462532124dba56c617782e3570087de4", "http://www.liblas.org/samples/ComplexSRSInfo.las"),
    (1, 2, 0, 0, 20, 497536, "b64b1632b5bf529b6ff7ebd75edcff7f5432f722", "http://www.liblas.org/samples/libLAS_1.2.las"),
    (1, 0, 0, 5, 20, 11781, "66f9d75c462532124dba56c617782e3570087de4", "http://www.liblas.org/samples/lt_srs_rt.las"),
    (1, 1, 0, 0, 20, 11781, "59f38a04c1aa3ad4989c30331e216e09b4f5aae8", "http://www.liblas.org/samples/lt_srs_rt-qt-1.1.las"),
    (1, 0, 1, 3, 28, 10, "db932601871eded2a3816573e481d810f86a4c72", "http://www.liblas.org/samples/srs.las")
  )

  val resources = "src/test/resources"
  new File(resources).mkdirs

  def download(url: String, uri: String, sha1: String) = {
    val file = new File(uri)
    // check SHA1
    val sha1ok = file.exists && file.isFile && file.length > 0 && {
      val data = new Array[Byte](file.length.toInt);
      val fin = new FileInputStream(file)
      val in = new DataInputStream(fin);
      try {
        in.readFully(data)
        val md = MessageDigest.getInstance("SHA-1")
        md.digest(data).map("%02x".format(_)).mkString == sha1
      } finally {
        try in.close finally fin.close
      }
    }
    if (!sha1ok) {
      file.delete
      print(s"Downloading $url ... ")
      try {
        //new URL(url) #> file !!;
        val u = new URL(url)
        val c = u.openConnection
        val contentLength = c.getContentLength
        val in = new BufferedInputStream(c.getInputStream)
        val data = new Array[Byte](contentLength);
        var bytesRead = 0
        var offset = 0
        while (offset < contentLength || bytesRead == -1) {
          bytesRead = in.read(data, offset, data.length - offset);
          offset += bytesRead;
        }
        in.close();

        val md = MessageDigest.getInstance("SHA-1")
        if (md.digest(data).map("%02x".format(_)).mkString == sha1) {
          val out = new FileOutputStream(file)
          out.write(data)
          out.flush
          out.close
          println("done")
        } else {
          println("wrong sha1, skipping")
        }
      } catch {
        case e: Throwable => file.delete; println(s"failed ($e), skipping")
      }
    }
    file
  }

  files foreach {
    case (major, minor, pdr_format, vlr_nb, pdr_length, pdf_nb, sha1, url) =>
      {
        val target = url.substring(url.lastIndexOf('/') + 1, url.length())
        val uri = s"$resources/$target"
        val file = download(url, uri, sha1)
        if (file.exists) {
          test(s"LAS$major.$minor/$pdr_format: $target should read the correct header metadata") {
            val header = LasHeader.read(file.getPath).get;
            header.version should equal(Version(major.toByte, minor.toByte))
            header.pdr_format should be(pdr_format)
            header.vlr_nb should be(vlr_nb)
            header.pdr_length should be(pdr_length)
            header.pdr_nb should be(pdf_nb)
            header.pdr_return_nb.sum should (be(header.pdr_nb) or be(0))
          }
        }
      }
  }
}
