package br.ufrn.dimap.forall.transmut.sbt.spark

import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin

import br.ufrn.dimap.forall.transmut.config._
import br.ufrn.dimap.forall.transmut.exception.ConfigurationException
import br.ufrn.dimap.forall.transmut.sbt.spark.process.TransmutSparkRDDProcess

object TransmutSparkRDDPlugin extends AutoPlugin {

  override def requires = JvmPlugin
  override def trigger = allRequirements

  object autoImport {
    lazy val transmut = taskKey[Unit]("Run TRANSMUT-Spark (Mutation Testing Process)")
    lazy val transmutConfigFile = settingKey[File]("TRANSMUT-Spark Configuration File")
    lazy val transmutConfig = taskKey[Config]("Load the TRANSMUT-Spark Configuration")
  }

  import autoImport._

  override def projectSettings = Seq(
    transmutConfigFile := baseDirectory.value / "transmut.conf",

    transmutConfig := transmutConfigTask.value,

    transmut := {
      implicit val config = transmutConfig.value
      val currentState = state.value
      val logger = streams.value.log
      val process = new TransmutSparkRDDProcess(currentState, logger)
      process.runProcess()
    })

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