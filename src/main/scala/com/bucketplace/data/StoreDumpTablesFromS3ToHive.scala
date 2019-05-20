package com.bucketplace.data

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.SparkSession


object StoreDumpTablesFromS3ToHive {
  val DUMP_DATABASE: String = "dump_test"
  val S3_PREFIX: String = "s3a://"

  def buildS3DumpPath(s3DumpPathPrefix: String, tableName: String, date: String): String =
    S3_PREFIX + s3DumpPathPrefix + "/dt=" + date + "/" + tableName + ".json"

  def buildS3HiveTablePath(s3HivePathPrefix: String, dbName: String, tableName: String): String =
    S3_PREFIX + s3HivePathPrefix + "/" + dbName + "/" + tableName

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
      .enableHiveSupport
      .getOrCreate

    val s3DumpPathPrefix = args(0)
    val s3HivePathPrefix = args(1)
    val tableName = args(2)
    val yesterday = args(3)

    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${DUMP_DATABASE}")

    val s3DumpPath = buildS3DumpPath(s3DumpPathPrefix, tableName, yesterday)
    val s3HiveTablePath = buildS3HiveTablePath(s3HivePathPrefix, DUMP_DATABASE, tableName)

    println("From S3 Dump: " + s3DumpPath)
    println("To S3 Hive Table: " + s3HiveTablePath)

    val tableDF = spark.read.json(s3DumpPath)
    tableDF
      .write
      .option("path", s3HiveTablePath)
      .mode("overwrite")
      .saveAsTable(s"${DUMP_DATABASE}.${tableName}")
  }
}
