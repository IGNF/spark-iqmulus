package fr.ign.spark.iqmulus.las

import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers

class LasSuite extends FunSuite with ShouldMatchers {

  test("data/libLAS_1.2.las should read the correct header metadata") {
    val header = LasHeader.read("data/libLAS_1.2.las").get;
    // val lasinfo = new java.io.FileOutputStream("data/libLAS_1.2_test.txt")
    // lasinfo.write(header.toString.getBytes)
    // lasinfo.close();
    header.version should equal(Array[Byte](1, 2))
    header.pdr_format should be(0)
    header.vlr_nb should be(0)
    header.pdr_length should be(20)
    header.pdr_nb should be(497536)
  }

}
