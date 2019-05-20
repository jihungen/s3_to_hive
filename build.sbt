// The simplest possible sbt build file is just one line:
  
scalaVersion := "2.12.8"
// That is, to create a valid sbt build, all you've got to do is define the
// version of Scala you'd like your project to use.

// ============================================================================

// Lines like the above defining `scalaVersion` are called "settings" Settings
// are key/value pairs. In the case of `scalaVersion`, the key is "scalaVersion"
// and the value is "2.12.8"

// It's possible to define many kinds of settings, such as:

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.8.5" % "provided"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.552" % "provided"

