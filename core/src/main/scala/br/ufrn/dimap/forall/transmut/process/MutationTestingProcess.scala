package br.ufrn.dimap.forall.transmut.process

import br.ufrn.dimap.forall.transmut.analyzer.ProgramBuilder
import br.ufrn.dimap.forall.transmut.report.Reporter
import br.ufrn.dimap.forall.transmut.config.Config
import br.ufrn.dimap.forall.transmut.mutation.manager.MutationManager
import br.ufrn.dimap.forall.transmut.mutation.manager.MetaMutantBuilder
import br.ufrn.dimap.forall.transmut.mutation.runner.MutantRunner
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantAnalyzer

trait MutationTestingProcess {

  def config: Config

  def reporter: Reporter

  def programBuilder: ProgramBuilder

  def mutantManager: MutationManager

  def metaMutantBuilder: MetaMutantBuilder

  def mutantRunner: MutantRunner

  def preProcess() {}

  def posProcess() {}

  def runProcess() {
    preProcess()
    reporter.reportProcessStart
    // Program Sources Build Process
    reporter.reportProgramBuildStart
    val programSources = programBuilder.buildProgramSources(config.sources, config.programs, config.transmutSrcDir, config.semanticdbDir)
    reporter.reportProgramBuildEnd(programSources)

    // Mutants and Meta-Mutant Generation Process
    reporter.reportMutantExecutionStart
    val metaMutants = programSources.map { programSource =>
      val mutants = mutantManager.generateMutantsFromProgramSource(programSource, config.mutationOperatorsList)
      metaMutantBuilder.buildMetaMutantProgramSourceFromMutantProgramSources(programSource, mutants)
    }
    metaMutants.foreach(metaMutant => metaMutantBuilder.writeMetaMutantToFile(metaMutant))
    reporter.reportMutantGenerationEnd(metaMutants)

    // Original Program and Mutants Execution
    reporter.reportMutantExecutionStart
    val metaMutantsTestResults = metaMutants.map(metaMutant => mutantRunner.runMutationTestProcess(metaMutant, config.equivalentMutants))
    val metaMutantsVerdicts = metaMutantsTestResults.map(m => MutantAnalyzer.analyzeMutants(m._1, m._2))
    reporter.reportMutantExecutionEnd(metaMutantsVerdicts)

    reporter.reportProcessEnd
    posProcess()
  }

}