package com.bucketplace.data

object BuildHiveTableFromS3DumpTable extends BuildHiveTableFromS3Trait {
    def main(args: Array[String]): Unit = {
        val s3DumpTablePathPrefix = args(0)
        val s3HivePathPrefix = args(1)
        val hiveDB = args(2)
        val tableName = args(3)
        val yesterday = args(4)

        implicit val spark = getSparkSession

        createDatabase(hiveDB)

        val s3DumpTablePath = buildS3DumpTablePath(s3DumpTablePathPrefix, tableName, yesterday)
        val s3HiveTablePath = buildS3HiveTablePath(s3HivePathPrefix, hiveDB, tableName)
        
        println("From S3 Dump: " + s3DumpTablePath)
        println("To S3 Hive Table: " + s3HiveTablePath)
        
        val tableDF = spark.read.json(s3DumpTablePath)
        tableDF
            .write
            .option("path", s3HiveTablePath)
            .mode("overwrite")
            .saveAsTable(s"${hiveDB}.${tableName}")
    }

    def buildS3DumpTablePath(s3DumpTablePathPrefix: String, tableName: String, date: String): String =
        S3_PREFIX + s3DumpTablePathPrefix + "/dt=" + date + "/" + tableName + ".json"
}
