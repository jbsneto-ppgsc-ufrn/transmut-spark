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
import br.ufrn.dimap.forall.transmut.report.CompoundReporter
import br.ufrn.dimap.forall.transmut.report.html.HTMLReporter
import br.ufrn.dimap.forall.transmut.report.json.JSONReporter

class TransmutSparkRDDProcess(state: State, logger: Logger)(implicit val config: Config) extends MutationTestingProcess {

  var runner = new SbtSparkRDDRunner(state)

  def reporter = CompoundReporter(ConsoleReporter(logger.info(_)), HTMLReporter(logger.info(_)), JSONReporter(logger.info(_)))

  def programBuilder = SparkRDDProgramBuilder

  def mutantManager = SparkRDDMutationManager
  
  def mutantReducer = SparkRDDMutantReducer

  def metaMutantBuilder = SparkRDDMetaMutantBuilder

  def mutantRunner = runner

  override def preProcess() {
    // Copy the content of the src directory to the transmut src directory
    IOFiles.copyDirectory(config.srcDir.toFile(), config.transmutSrcDir.toFile())
  }

  override def posProcess() {
    // Copy only the meta mutants to the transmut mutants directory
    metaMutants.foreach { meta =>
      IOFiles.copyFile(meta.mutated.source.toFile(), config.transmutMutantsDir.toFile())
    }
    reporter.reportAdditionalInformation("Meta Mutants copied to " + config.transmutMutantsDir.toString())
  }

}