import sbt._

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

semanticdbEnabled := true
semanticdbVersion := "4.3.0"
semanticdbIncludeInJar := false

TaskKey[Unit]("checkBeforeTransmut") := {
    val targetFolder = target.value
    val listFilesInTarget = IO.listFiles(targetFolder)
    val transmutFolders = listFilesInTarget.filter(f => f.isDirectory() && f.getName.contains("transmut-"))
    assert(transmutFolders.isEmpty)
}

TaskKey[Unit]("checkAfterTransmut") := {
    val targetFolder = target.value
    val listFilesInTarget = IO.listFiles(targetFolder)
    val transmutFolders = listFilesInTarget.filter(f => f.isDirectory() && f.getName.contains("transmut-"))
    assert(!transmutFolders.isEmpty)
    assert(transmutFolders.size == 1)
    val transmutFolder = transmutFolders.head
    val transmutFolderFiles = IO.listFiles(transmutFolder)
    assert(transmutFolderFiles.filter(f => f.getName.contains("mutants") && f.isDirectory()).size == 1)
    assert(transmutFolderFiles.filter(f => f.getName.contains("mutated-src") && f.isDirectory()).size == 1)
    assert(transmutFolderFiles.filter(f => f.getName.contains("reports") && f.isDirectory()).size == 1)
    val reportsFolder = transmutFolderFiles.filter(f => f.getName.contains("reports") && f.isDirectory()).head
    val reportsFolderFiles = IO.listFiles(reportsFolder)
    assert(reportsFolderFiles.filter(f => f.isDirectory()).size == 2)
    val htmlReportsFolder = reportsFolderFiles.filter(f => f.getName.contains("html") && f.isDirectory()).head
    val htmlIndexFile = new File(htmlReportsFolder, "index.html")
    assert(htmlIndexFile.exists() && htmlIndexFile.isFile())
    val jsonReportsFolder = reportsFolderFiles.filter(f => f.getName.contains("json") && f.isDirectory()).head
    val jsonIndexFile = new File(jsonReportsFolder, "Mutation-Testing-Process.json")
    assert(jsonIndexFile.exists() && jsonIndexFile.isFile())

    // Check if the reduction has been applied (that is, there must be files in the RemovedMutants folder)
    val htmlReportsFolderFiles = IO.listFiles(htmlReportsFolder)
    assert(htmlReportsFolderFiles.filter(f => f.isDirectory()).size == 4)
    val htmlRemovedMutantsReportsFolder = htmlReportsFolderFiles.filter(f => f.getName.contains("RemovedMutants") && f.isDirectory()).head
    val htmlRemovedMutantsReportsFolderFiles = IO.listFiles(htmlRemovedMutantsReportsFolder)
    assert(htmlRemovedMutantsReportsFolderFiles.size == 1)

    val jsonReportsFolderFiles = IO.listFiles(jsonReportsFolder)
    assert(jsonReportsFolderFiles.filter(f => f.isDirectory()).size == 4)
    val jsonRemovedMutantsReportsFolder = jsonReportsFolderFiles.filter(f => f.getName.contains("RemovedMutants") && f.isDirectory()).head
    val jsonRemovedMutantsReportsFolderFiles = IO.listFiles(jsonRemovedMutantsReportsFolder)
    assert(jsonRemovedMutantsReportsFolderFiles.size == 1)
}