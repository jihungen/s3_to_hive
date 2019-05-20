package com.bucketplace.data

object BuildHiveTableFromS3DumpTable extends BuildHiveTableFromS3Trait {
    val DUMP_DATABASE: String = "dump_test"

    def main(args: Array[String]): Unit = {
        val s3DumpPathPrefix = args(0)
        val s3HivePathPrefix = args(1)
        val tableName = args(2)
        val yesterday = args(3)

        implicit val spark = getSparkSession

        createDatabase(DUMP_DATABASE)

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
