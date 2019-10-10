

lazy val global = (project in file("."))
  .settings(
   name := "mutation-transformation-testing-tool",
   commonSettings
  )
  .aggregate(
    util,
    codeAnalyzer
  )

lazy val util = (project in file("util"))
  .settings(
    name := "util",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )

lazy val codeAnalyzer = (project in file("code-analyzer"))
  .settings(
    name := "code-analyzer",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )
  .dependsOn(util)

lazy val commonDependencies = Seq(
  "org.scalameta" %% "scalameta" % "4.2.0",
  "org.scalameta" %% "semanticdb" % "4.1.0",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

lazy val commonSettings = Seq(
  organization := "br.ufrn.dimap.forall",
  scalaVersion := "2.12.8",
  version := "0.1",
  fork in Test := true,
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
  parallelExecution in Test := false
)

