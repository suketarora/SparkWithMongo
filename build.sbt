name := "SparkWithMongo"

version := "0.1"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0"
)

val projectMainClass = "com.mongodb.GettingStarted"

mainClass in (Compile, run) := Some(projectMainClass)