package br.ufrn.dimap.forall.transmut.sbt.spark

import sbt._
import sbt.Keys._
import br.ufrn.dimap.forall.transmut.config.Config

trait TransmutSparkRDDKeys {
  
  lazy val transmut = taskKey[Unit]("Run TRANSMUT-Spark (Mutation Testing Process)")
  
  lazy val transmutAlive = taskKey[Unit]("Run TRANSMUT-Spark (Mutation Testing Process) Only For Live Mutants From The Previous Run")
  
  lazy val transmutConfig = taskKey[Config]("Load the TRANSMUT-Spark Configuration")
  
  lazy val transmutConfigFile = settingKey[File]("TRANSMUT-Spark Configuration File")
  
  lazy val transmutTest = taskKey[Unit]("TRANSMUT-Spark Custom Test Task") in Test
  
}