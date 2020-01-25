package br.ufrn.dimap.forall.transmut.sbt.spark.process

import br.ufrn.dimap.forall.transmut.config.Config
import sbt._
import sbt.Keys._
import br.ufrn.dimap.forall.transmut.process.MutationTestingProcess
import br.ufrn.dimap.forall.transmut.report.ConsoleReporter
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.analyzer.ProgramBuilder
import br.ufrn.dimap.forall.transmut.spark.mutation.manager.SparkRDDMutationManager
import br.ufrn.dimap.forall.transmut.mutation.manager.MetaMutantBuilder
import br.ufrn.dimap.forall.transmut.mutation.manager.MutationManager
import br.ufrn.dimap.forall.transmut.mutation.runner.MutantRunner
import br.ufrn.dimap.forall.transmut.spark.mutation.manager._
import br.ufrn.dimap.forall.transmut.sbt.spark.runner.SbtSparkRDDRunner
import br.ufrn.dimap.forall.transmut.util.IOFiles

class TransmutSparkRDDProcess(state: State)(implicit val config: Config) extends MutationTestingProcess {

  var runner = new SbtSparkRDDRunner(state)
  
  def reporter = ConsoleReporter

  def programBuilder = SparkRDDProgramBuilder

  def mutantManager = SparkRDDMutationManager

  def metaMutantBuilder = SparkRDDMetaMutantBuilder

  def mutantRunner = runner

  override def preProcess() {
    // Copy the content of the src directory to the transmut src directory
    IOFiles.copyDirectory(config.srcDir.toFile(), config.transmutSrcDir.toFile())
  }

  override def posProcess() {}

}