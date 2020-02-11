name := "transmut-plugin-tests"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % "test"
)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

semanticdbEnabled := false
semanticdbVersion := "4.3.0"
semanticdbIncludeInJar := false