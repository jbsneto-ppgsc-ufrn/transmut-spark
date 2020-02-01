package br.ufrn.dimap.forall.transmut.process

import br.ufrn.dimap.forall.transmut.analyzer.ProgramBuilder
import br.ufrn.dimap.forall.transmut.report.Reporter
import br.ufrn.dimap.forall.transmut.config.Config
import br.ufrn.dimap.forall.transmut.mutation.manager.MutationManager
import br.ufrn.dimap.forall.transmut.mutation.manager.MetaMutantBuilder
import br.ufrn.dimap.forall.transmut.mutation.runner.MutantRunner
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantAnalyzer
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutant
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.runner.TestResult
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult

trait MutationTestingProcess {

  def config: Config

  def reporter: Reporter

  def programBuilder: ProgramBuilder

  def mutantManager: MutationManager

  def metaMutantBuilder: MetaMutantBuilder

  def mutantRunner: MutantRunner

  var _programSources: List[ProgramSource] = _
  var _metaMutants: List[MetaMutantProgramSource] = _
  var _metaMutantsTestResults: List[(MetaMutantProgramSource, List[TestResult[MutantProgramSource]])] = _
  var _metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])] = _

  def programSources = _programSources
  
  def programSources_=(sources: List[ProgramSource]) {
    _programSources = sources
  }
  
  def metaMutants = _metaMutants

  def metaMutants_=(meta: List[MetaMutantProgramSource]) {
    _metaMutants = meta
  }
  
  def metaMutantsTestResults = _metaMutantsTestResults
  
  def metaMutantsTestResults_=(testResults: List[(MetaMutantProgramSource, List[TestResult[MutantProgramSource]])]){
    _metaMutantsTestResults = testResults
  }
  
  def metaMutantsVerdicts = _metaMutantsVerdicts
  
  def metaMutantsVerdicts_=(mutantsResults: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]){
    _metaMutantsVerdicts = mutantsResults
  }
  
  def preProcess() {}

  def posProcess() {}

  def runProcess() {
    preProcess()
    reporter.reportProcessStart
    // Program Sources Build Process
    reporter.reportProgramBuildStart
    programSources = programBuilder.buildProgramSources(config.sources, config.programs, config.transmutSrcDir, config.semanticdbDir)
    reporter.reportProgramBuildEnd(programSources)

    // Mutants and Meta-Mutant Generation Process
    reporter.reportMutantExecutionStart
    val mutantsIdGenerator = mutantManager.defaultMutantsIdGenerator // for each mutant to have a unique id
    metaMutants = programSources.map { programSource =>
      val mutants = mutantManager.generateMutantsFromProgramSource(programSource, config.mutationOperatorsList, mutantsIdGenerator)
      metaMutantBuilder.buildMetaMutantProgramSourceFromMutantProgramSources(programSource, mutants)
    }
    metaMutants.foreach(metaMutant => metaMutantBuilder.writeMetaMutantToFile(metaMutant))
    reporter.reportMutantGenerationEnd(metaMutants)

    // Original Program and Mutants Execution
    reporter.reportMutantExecutionStart
    metaMutantsTestResults = metaMutants.map(metaMutant => mutantRunner.runMutationTestProcess(metaMutant, config.equivalentMutants))
    metaMutantsVerdicts = metaMutantsTestResults.map(m => MutantAnalyzer.analyzeMutants(m._1, m._2, config.equivalentMutants))
    reporter.reportMutantExecutionEnd(metaMutantsVerdicts)

    reporter.reportProcessEnd
    posProcess()
  }

}