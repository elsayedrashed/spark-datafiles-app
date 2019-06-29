package main.scala

import org.scalatest.FunSuite

class CountCSVFileRecordsTest extends FunSuite {

  test("Test Case : Count CSV File Records") {

    var appName = "E2K"
    val sparkMaster = "local[*]"
    val csvDelimiter = "|"
    val csvFile = "src/test/resources/inputall/*.csv"

    CountCSVFileRecords.main(Array(appName,sparkMaster,csvDelimiter,csvFile))
    assert(1 === 1)
  }
}
