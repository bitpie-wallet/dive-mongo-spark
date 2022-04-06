version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"

lazy val root = (project in file("."))
  .settings(
    name := "dive-mongo",
    idePackagePrefix := Some("dive.address")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
)
