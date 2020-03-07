name := "transmut-spark-example-project"

version := "0.1"

scalaVersion := "2.12.7"

val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % "test"
)

fork in Test := false
parallelExecution in Test := false

javaOptions in Compile ++= Seq("-Xms1G", "-Xmx4G", "-XX:MaxPermSize=4G", "-XX:+CMSClassUnloadingEnabled")

javaOptions in Test ++= Seq("-Xms2G", "-Xmx4G", "-XX:MaxPermSize=4G", "-XX:+CMSClassUnloadingEnabled")

semanticdbEnabled := true
semanticdbVersion := "4.1.0"
semanticdbIncludeInJar := false