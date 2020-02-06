lazy val global = (project in file("."))
  .settings(
   name := "mutation-transformation-testing-tool",
   commonSettings
  )
  .aggregate(
    core,
    util,
    codeAnalyzer,
    mutationManager,
    mutationAnalyzer, 
    sbtTransmut
  ) 

lazy val sbtTransmut = (project in file("sbt-transmut"))
  .enablePlugins(SbtPlugin)
  .settings(
    name := "sbt-transmut",
    commonSettings,
    libraryDependencies ++= commonDependencies,
    scriptedLaunchOpts := { scriptedLaunchOpts.value ++
      Seq("-Xmx1024M", "-Dplugin.version=" + version.value)
    },
    scriptedBufferLog := false
  )
  .dependsOn(util, codeAnalyzer, mutationManager, mutationAnalyzer, core)

lazy val core = (project in file("core"))
  .settings(
    name := "core",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )
  .dependsOn(util, codeAnalyzer, mutationManager, mutationAnalyzer)

lazy val codeAnalyzer = (project in file("code-analyzer"))
  .settings(
    name := "code-analyzer",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )
  .dependsOn(util)

lazy val mutationManager = (project in file("mutation-manager"))
  .settings(
    name := "mutation-manager",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )
  .dependsOn(util, codeAnalyzer)

lazy val mutationAnalyzer = (project in file("mutation-analyzer"))
  .settings(
    name := "mutation-analyzer",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )
  .dependsOn(util, codeAnalyzer, mutationManager)

lazy val util = (project in file("util"))
  .settings(
    name := "util",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )

lazy val commonDependencies = Seq(
  "io.circe" %% "circe-core" % "0.12.3",
  "io.circe" %% "circe-generic" % "0.12.3",
  "io.circe" %% "circe-parser" % "0.12.3",
  "com.github.pureconfig" %% "pureconfig" % "0.12.2",
  "org.scalameta" %% "scalameta" % "4.2.0",
  "org.scalameta" %% "semanticdb" % "4.1.0",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

lazy val commonSettings = Seq(
  organization := "br.ufrn.dimap.forall",
  scalaVersion := "2.12.8",
  version := "0.1-SNAPSHOT",
  fork in Test := true,
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
  parallelExecution in Test := false
)

