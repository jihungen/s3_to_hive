package com.bucketplace.data

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions

object BuildHiveTableFromS3Log extends BuildHiveTableFromS3Trait {
    val LOG_DATABASE: String = "log_test"

    def main(args: Array[String]): Unit = {
        val s3DumpPathPrefix = args(0)
        val s3HivePathPrefix = args(1)
        val hiveDB = args(2)
        val tableName = args(3)
        val yesterday = args(4)

        implicit val spark = getSparkSession

        createDatabase(hiveDB)

        val s3DumpPath = buildS3DumpPath(s3DumpPathPrefix, yesterday)
        val s3HiveTablePath = buildS3HiveTablePath(s3HivePathPrefix, hiveDB, tableName)
        
        println("From S3 Dump: " + s3DumpPath)
        println("To S3 Hive Table: " + s3HiveTablePath)

        val formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd")
        val yesterdayLocalDate = LocalDate.parse(yesterday, formatter)
        
        val tableDF = spark.read.json(s3DumpPath)
        val tableWithDateDF = tableDF
            .withColumn("year", functions.lit(yesterdayLocalDate.getYear))
            .withColumn("month", functions.lit(yesterdayLocalDate.getMonthValue))
            .withColumn("day", functions.lit(yesterdayLocalDate.getDayOfMonth))

        if (isTableExist(hiveDB, tableName))
            tableWithDateDF.write.insertInto(s"${hiveDB}.${tableName}")
        else
            tableWithDateDF
                .write
                .option("path", s3HiveTablePath)
                .partitionBy("year", "month", "day")
                .saveAsTable(s"${hiveDB}.${tableName}")
  }
}
