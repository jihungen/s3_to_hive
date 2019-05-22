package com.bucketplace.data

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions

object BuildHiveTableFromS3Log extends BuildHiveTableFromS3Trait {
    def main(args: Array[String]): Unit = {
        val s3LogPathPrefix = args(0)
        val s3HivePathPrefix = args(1)
        val hiveDB = args(2)
        val tableName = args(3)
        val yesterday = args(4)

        implicit val spark = getSparkSession

        createDatabase(hiveDB)

        val s3LogPath = buildS3LogPath(s3LogPathPrefix, yesterday)
        val s3HiveTablePath = buildS3HiveTablePath(s3HivePathPrefix, hiveDB, tableName)
        
        println("From S3 Dump: " + s3LogPath)
        println("To S3 Hive Table: " + s3HiveTablePath)

        val formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd")
        val yesterdayLocalDate = LocalDate.parse(yesterday, formatter)
        
        val tableDF = spark.read.json(s3LogPath)
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

    def buildS3LogPath(s3DumpPath: String, date: String): String =
        S3_PREFIX + s3DumpPath + "/dt=" + date
}
