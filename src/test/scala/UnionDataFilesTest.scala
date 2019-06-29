package main.scala

import org.scalatest.FunSuite


class UnionDataFilesTest extends FunSuite {

  test("Test Case : Union Data Files") {

    val singleFile = "M"
    val csvDelimiter = "|"
    var appName = "E2K"
    val sparkMaster = "local[*]"
    val masterFile = "src/test/resources/input/fullload.csv"
    val deltaFile = "src/test/resources/input/delta.csv"
    val outputPath = "src/test/resources/output/union"

    UnionDataFiles.main(Array(singleFile,csvDelimiter,appName,sparkMaster,masterFile,deltaFile,outputPath))
    assert(1 === 1)
  }
}
