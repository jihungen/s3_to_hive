package com.bucketplace.data

import org.apache.spark.sql.SparkSession

trait BuildHiveTableFromS3Trait {
    val S3_PREFIX: String = "s3a://"

    def getSparkSession: SparkSession = 
        SparkSession
            .builder
            .config(
                "hive.metastore.client.factory.class", 
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            ).enableHiveSupport
            .getOrCreate

    def buildS3DumpPath(s3DumpPathPrefix: String, tableName: String, date: String): String =
        S3_PREFIX + s3DumpPathPrefix + "/dt=" + date + "/" + tableName + ".json"

    def buildS3DumpPath(s3DumpPath: String, date: String): String =
        S3_PREFIX + s3DumpPath + "/dt=" + date
        
    def buildS3HiveTablePath(s3HivePathPrefix: String, dbName: String, tableName: String): String =
        S3_PREFIX + s3HivePathPrefix + "/" + dbName + "/" + tableName

    def createDatabase(dbName: String)(implicit spark: SparkSession): Unit = 
        spark.sql(s"CREATE DATABASE IF NOT EXISTS ${dbName}")

    def isTableExist(dbName: String, tableName: String)(implicit spark: SparkSession): Boolean =
        spark.catalog.tableExists(dbName, tableName)
}
