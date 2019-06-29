name := "spark-datafiles-app"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.3"
val AvroVersion = "4.0.0"
val ScalaTestVersion = "3.0.8"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

// Apache Spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources()
)

// Avro Library
libraryDependencies += "com.databricks" %% "spark-avro" % AvroVersion

// Test Framework
logBuffered in Test := false
libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % ScalaTestVersion,
  "org.scalatest" %% "scalatest" % ScalaTestVersion % "test"
)
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest,
  "-y", "org.scalatest.FunSuite",
  "-y", "org.scalatest.FunSpec",
  "-y", "org.scalatest.PropSpec",
  "-y", "org.scalatest.FlatSpec",
  "-y", "org.scalatest.FeatureSpec"
)

// Assembly
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x => MergeStrategy.first
}

