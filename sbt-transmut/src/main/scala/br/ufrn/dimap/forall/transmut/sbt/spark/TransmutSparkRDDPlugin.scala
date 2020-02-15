package br.ufrn.dimap.forall.transmut.sbt.spark

import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin

import br.ufrn.dimap.forall.transmut.config._
import br.ufrn.dimap.forall.transmut.exception.ConfigurationException
import br.ufrn.dimap.forall.transmut.sbt.spark.process.TransmutSparkRDDProcess
import br.ufrn.dimap.forall.transmut.report.json.MutationTestingProcessJSONReport
import br.ufrn.dimap.forall.transmut.report.json.model.MutationTestingProcessJSON
import br.ufrn.dimap.forall.transmut.util.DateTimeUtil
import br.ufrn.dimap.forall.transmut.util.IOFiles

object TransmutSparkRDDPlugin extends AutoPlugin {

  override def requires = JvmPlugin
  override def trigger = allRequirements

  object autoImport extends TransmutSparkRDDKeys

  import autoImport._

  override def projectSettings = Seq(
    transmut := transmutTask.value,

    transmutAlive := transmutAliveTask.value,

    transmutConfig := transmutConfigTask.value,

    transmutConfigFile := baseDirectory.value / "transmut.conf",

    transmutTest := transmutTestTask.value)

  def transmutTask = Def.task {
    (compile in Compile).value // compile the project to generate semanticdb specifications
    implicit val config = transmutConfig.value
    val currentState = state.value
    val logger = streams.value.log
    val configFile = transmutConfigFile.value
    IOFiles.copyFile(configFile, config.transmutDir.toFile()) // Creates a copy of the transmut config file into the transmut folder
    logger.info("TRANSMUT-Spark configuration filed copied to " + config.transmutDir.toString())
    val process = new TransmutSparkRDDProcess(currentState, logger)
    try {
      process.runProcess()
    } catch {
      case e: Throwable => {
        // The transmut directory is deleted because execution failed
        IOFiles.deleteFile(config.transmutDir.toFile())
        logger.error("The TRANSMUT-Spark process failed, the folder " + config.transmutDir.toString + " was deleted!")
        // To indicate that the task has failed
        throw e
      }
    }
  }

  def transmutAliveTask = Def.task {
    (compile in Compile).value // compile the project to generate semanticdb specifications
    implicit val config = transmutConfig.value
    val logger = streams.value.log
    val currentState = state.value
    var runProcess = true
    val listFiles = IO.listFiles(config.transmutBaseDir.toFile())
    // Get the list of previous transmut folders inside transmutBaseDir
    val transmutFolders = listFiles.filter(f => f.isDirectory() && f.getName.contains("transmut-"))
    // Checks if there is at least one previous transmut folder
    if (!transmutFolders.isEmpty) {
      // Get the list of previous transmut folders with the date of each one and sorted with the date
      val transmutFolderDateTime = transmutFolders.map { folder =>
        val datestamp = folder.getName.replace("transmut-", "")
        val dateTime = DateTimeUtil.getDateTimeFromDatestamp(datestamp)
        (folder, dateTime)
      }.toList.sortWith((a, b) => a._2.isBefore(b._2))
      // Get the last transmut folder (the newest one)
      val lastTransmutFolder = transmutFolderDateTime.last._1
      // Get the last configuration
      val lastConfigFile = new File(lastTransmutFolder, "transmut.conf")
      val lastConfig = ConfigReader.readConfig(lastConfigFile)
      // Check if sources, programs and mutation-operators of the last configuration are the same of the current configuration, this is mandatory to run the living mutants again
      if (config.sources == lastConfig.sources && config.programs == lastConfig.programs && config.mutationOperators == lastConfig.mutationOperators) {
        // Check if equivalent-mutants of the last configuration is a subset of the equivalent-mutants of the current configuration, this avoids inconsistency to run the living mutants again (new ones can be added, but old ones need to remain)
        if (lastConfig.equivalentMutants.toSet.subsetOf(config.equivalentMutants.toSet)) {
          val directory = new File(lastTransmutFolder, "reports/json")
          val fileName = "Mutation-Testing-Process.json"
          val mutationTestingProcessJSONFile = new File(directory, fileName)
          // Check if the JSON report from the last run exists
          if (directory.exists() && mutationTestingProcessJSONFile.exists()) {
            // Read the previous JSON report
            val mutationTestingProcessJSON = MutationTestingProcessJSONReport.readMutationTestingProcessJSONReportFile(mutationTestingProcessJSONFile)
            // Get the list of living mutants from previous run
            val livingMutantsIds = mutationTestingProcessJSON.mutants.filter(m => m.status == "Survived").map(m => m.id)
            // Check if there is any living mutant to run the process
            if (!livingMutantsIds.isEmpty) {
              runProcess = true
              config.livingMutants = livingMutantsIds
              config.testLivingMutants = true
              logger.info("List of living mutants from previous run of TRANSMUT-Spark that will run again: " + livingMutantsIds.mkString(", "))
            } else {
              // Not exist living mutants from previous run
              runProcess = false
              logger.warn("There are no surviving mutants from the previous execution of TRANSMUT-Spark.")
              logger.warn("The process is not going to run, execute the 'transmut' task to run the whole process again.")
            }
          } else {
            // The JSON report from previous run was not found
            runProcess = false
            logger.warn("The JSON report of the last execution of TRANSMUT-Spark was not found, it is necessary to execute the living mutants from the last execution of TRANSMUT-Spark.")
            logger.warn("The process is not going to run, execute the 'transmut' task to run the whole process again.")
          }
        } else {
          // The equivalent-mutants from previous run are not a subset of the current equivalent-mutants
          runProcess = false
          logger.warn("The list of equivalent mutants in 'transmut.conf' does not have the equivalent mutants that were in the list of the configuration in the last execution of TRANSMUT-Spark. Other equivalent mutants can be added to the equivalent mutants list, but those on the previous list must remain on the current list to run the living mutants again!")
          logger.warn("Open " + lastConfigFile.toString() + " to see the difference with the current configuration.")
          logger.warn("The process is not going to run, fix the configurations and execute the 'transmutAlive' task again or execute the 'transmut' task to run the whole process again.")
        }
      } else {
        // The sources, programs or mutation-operators of the last configuration were different from those of the current one
        runProcess = false
        logger.warn("The sources, programs, and/or mutation operators in 'transmut.conf' are different from those of the configuration in the last execution of TRANSMUT-Spark.")
        logger.warn("Open " + lastConfigFile.toString() + " to see the difference with the current configuration.")
        logger.warn("The process is not going to run, fix the configurations and execute the 'transmutAlive' task again or execute the 'transmut' task to run the whole process again.")
      }
    } else {
      // No previous transmut folder was found.
      logger.warn("TRANSMUT-Spark has not been previously run, the whole process will run!")
    }
    if (runProcess) {
      val configFile = transmutConfigFile.value
      IOFiles.copyFile(configFile, config.transmutDir.toFile()) // Creates a copy of the transmut config file into the transmut folder
      logger.info("TRANSMUT-Spark configuration filed copied to " + config.transmutDir.toString())
      val process = new TransmutSparkRDDProcess(currentState, logger)
      try {
        process.runProcess()
      } catch {
        case e: Throwable => {
          // The transmut directory is deleted because execution failed
          IOFiles.deleteFile(config.transmutDir.toFile())
          logger.error("The TRANSMUT-Spark process failed, the folder " + config.transmutDir.toString + " was deleted!")
          // To indicate that the task has failed
          throw e
        }
      }
    }
  }

  // Custom test task that runs testOnly if it is enabled (testOnly in config is not empty) or runs test (all tests) otherwise
  def transmutTestTask = Def.taskDyn {
    val config = transmutConfig.value
    if (config.isTestOnlyEnabled)
      Def.task { (testOnly in Test).toTask(config.testOnlyParameters).value }
    else
      Def.task { (test in Test).value }
  }

  val transmutConfigTask = Def.task {
    val isSemanticdbEnabled = (semanticdbEnabled in Compile).value
    if (isSemanticdbEnabled) {
      val config = ConfigReader.readConfig(transmutConfigFile.value)
      // Adjust default values
      if (config.srcDir == config.defaultSrcDir) {
        config.srcDir = (scalaSource in Compile).value.toPath()
      }
      if (config.semanticdbDir == config.defaultSemanticdbDir) {
        config.semanticdbDir = (semanticdbTargetRoot in Compile).value.toPath()
      }
      if (config.transmutBaseDir == config.defaultTransmutBaseDir) {
        config.transmutBaseDir = (target.value).toPath()
      }
      config.updateProcessStartTimeToNow
      config
    } else {
      throw new ConfigurationException("SemanticDB is not enabled, TRANSMUT-Spark is dependent on it to obtain semantic information")
    }
  }

}