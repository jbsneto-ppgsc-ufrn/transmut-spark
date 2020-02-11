package br.ufrn.dimap.forall.transmut.sbt.spark

import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin

import br.ufrn.dimap.forall.transmut.config._
import br.ufrn.dimap.forall.transmut.exception.ConfigurationException
import br.ufrn.dimap.forall.transmut.sbt.spark.process.TransmutSparkRDDProcess
import br.ufrn.dimap.forall.transmut.report.json.MutationTestingProcessJSONReport

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
    val process = new TransmutSparkRDDProcess(currentState, logger)
    process.runProcess()
  }

  def transmutAliveTask = Def.task {
    (compile in Compile).value // compile the project to generate semanticdb specifications
    implicit val config = transmutConfig.value
    val logger = streams.value.log
    val currentState = state.value
    val directory = config.transmutJSONReportsDir.toFile()
    val fileName = "Mutation-Testing-Process.json"
    val mutationTestingProcessJSONFile = new File(directory, fileName)
    var runProcess = true
    if (directory.exists() && mutationTestingProcessJSONFile.exists()) {
      val mutationTestingProcessJSON = MutationTestingProcessJSONReport.readMutationTestingProcessJSONReportFile(mutationTestingProcessJSONFile)
      val livingMutantsIds = mutationTestingProcessJSON.mutants.filter(m => m.status == "Survived").map(m => m.id)
      if (!livingMutantsIds.isEmpty) {
        logger.info("List of living mutants from previous run of TRANSMUT-Spark that will run again: " + livingMutantsIds.mkString(", "))
        config.livingMutants = livingMutantsIds
        config.testLivingMutants = true
      } else {
        logger.info("There are no surviving mutants from the previous execution of TRANSMUT-Spark, the process is not going to run (Execute the 'transmut' task to run the whole process again)")
        runProcess = false
      }
    } else {
      logger.info("TRANSMUT-Spark has not been previously run, the whole process will run!")
    }
    if (runProcess) {
      val process = new TransmutSparkRDDProcess(currentState, logger)
      process.runProcess()
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
      if (config.transmutDir == config.defaultTransmutDir) {
        config.transmutDir = (target.value / "transmut").toPath()
      }
      config
    } else {
      throw new ConfigurationException("SemanticDB is not enabled, TRANSMUT-Spark is dependent on it to obtain semantic information")
    }
  }

}