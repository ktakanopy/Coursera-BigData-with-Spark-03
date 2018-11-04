package timeusage

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql._

import scala.util.Random
import org.apache.spark.sql.types._
import timeusage.TimeUsage.spark

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
  // For implicit conversions like converting RDDs to DataFrames

  import spark.implicits._
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  test("TimeUsageSuite ") {

    val primaryList = List("t0101","t180304")
    val workList = List("t0505")
    val otherList = List("t1303","t1516","t12")

    val expected1 : List[Column] = primaryList.map(c => col(c))
    val expected2 : List[Column] = workList.map(c => col(c))
    val expected3 : List[Column] = otherList.map(c => col(c))


    val (res1,res2,res3) = TimeUsage.classifiedColumns(primaryList ++ workList ++ otherList)

    println("res1 = " + res1.toString())
    println("exoected1 = " + expected1.toString())

    println("res2 = " + res2.toString())
    println("exoected2 = " + expected2.toString())

    println("res3 = " + res3.toString())
    println("exoected3 = " + expected3.toString())

    assert(res1 === expected1 && res2 === expected2 && res3 == expected3)
  }

  val summed =
    Seq(
      TimeUsageRow("working", "female", "active", 7.667, 0.62, 5.153),
      TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111),
      TimeUsageRow("working", "female", "active", 7.667, 0.62, 5.153),
      TimeUsageRow("not working", "male", "young", 4.42, 0.53, 5.7),
      TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111),
      TimeUsageRow("working", "female", "young", 13.9501, 8.97, 0.0444),
      TimeUsageRow("not working", "male", "young", 4.42, 0.53, 5.7),
      TimeUsageRow("not working", "male", "young", 4.42, 0.53, 5.7),
      TimeUsageRow("not working", "male", "young", 4.42, 0.53, 5.7),
      TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111),
      TimeUsageRow("working", "female", "young", 13.9501, 8.97, 0.0444),
      TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111),
      TimeUsageRow("working", "female", "young", 13.9501, 8.97, 0.0444),
      TimeUsageRow("not working", "male", "young", 4.42, 0.53, 5.7),
      TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111),
      TimeUsageRow("working", "female", "young", 13.9501, 8.97, 0.0444),
      TimeUsageRow("working", "female", "active", 7.667, 0.62, 5.153),
      TimeUsageRow("working", "female", "young", 13.9501, 8.97, 0.0444),
      TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111)
    ).toDF
  val expected =
    Seq(
      TimeUsageRow("not working", "male", "young", 4.4, 0.5, 5.7),
      TimeUsageRow("working", "female", "active", 7.7, 0.6, 5.2),
      TimeUsageRow("working", "female", "young", 14.0, 9.0, 0.0),
      TimeUsageRow("working", "male", "elder", 11.0, 3.0, 5.7)
    ).toDF

  test(">>> using DataFrame API") {
    val result = TimeUsage.timeUsageGrouped(summed)
    assert(result.collect.toSeq == expected.collect.toSeq, "result and expected dataframes should be equal")
  }

}
