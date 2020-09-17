package br.ufrn.dimap.forall.transmut.process

import br.ufrn.dimap.forall.transmut.analyzer.ProgramBuilder
import br.ufrn.dimap.forall.transmut.config.Config
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantAnalyzer
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.manager.MetaMutantBuilder
import br.ufrn.dimap.forall.transmut.mutation.manager.MutationManager
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved
import br.ufrn.dimap.forall.transmut.mutation.runner.MutantRunner
import br.ufrn.dimap.forall.transmut.mutation.runner.TestResult
import br.ufrn.dimap.forall.transmut.report.Reporter
import br.ufrn.dimap.forall.transmut.mutation.manager.MutantReducer

trait MutationTestingProcess {

  def config: Config

  def reporter: Reporter

  def programBuilder: ProgramBuilder

  def mutantManager: MutationManager

  def mutantReducer: MutantReducer

  def metaMutantBuilder: MetaMutantBuilder

  def mutantRunner: MutantRunner

  var _programSources: List[ProgramSource] = _
  var _removedMutants: List[MutantRemoved] = _
  var _metaMutants: List[MetaMutantProgramSource] = _
  var _metaMutantsTestResults: List[(MetaMutantProgramSource, List[TestResult[MutantProgramSource]])] = _
  var _metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])] = _

  def programSources = _programSources

  def programSources_=(sources: List[ProgramSource]) {
    _programSources = sources
  }

  def removedMutants = _removedMutants

  def removedMutants_=(muts: List[MutantRemoved]) {
    _removedMutants = muts
  }

  def metaMutants = _metaMutants

  def metaMutants_=(meta: List[MetaMutantProgramSource]) {
    _metaMutants = meta
  }

  def metaMutantsTestResults = _metaMutantsTestResults

  def metaMutantsTestResults_=(testResults: List[(MetaMutantProgramSource, List[TestResult[MutantProgramSource]])]) {
    _metaMutantsTestResults = testResults
  }

  def metaMutantsVerdicts = _metaMutantsVerdicts

  def metaMutantsVerdicts_=(mutantsResults: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) {
    _metaMutantsVerdicts = mutantsResults
  }

  def preProcess() {}

  def posProcess() {}

  def runProcess() {
    preProcess()
    reporter.reportProcessStart(config.processStartDateTime)
    // Program Sources Build Process
    reporter.reportProgramBuildStart
    programSources = programBuilder.buildProgramSources(config.sources, config.programs, config.transmutSrcDir, config.semanticdbDir)
    reporter.reportProgramBuildEnd(programSources)

    // Mutants and Meta-Mutant Generation Process
    reporter.reportMutantGenerationStart
    val mutantsIdGenerator = mutantManager.defaultMutantsIdGenerator // for each mutant to have a unique id
    
    // Initializes the removed mutants list variable
    if (removedMutants == null) removedMutants = Nil
    
    metaMutants = programSources.map { programSource =>
      var mutants = mutantManager.generateMutantsFromProgramSource(programSource, config.mutationOperatorsList, mutantsIdGenerator)

      // Mutants Reduction Process
      if (config.enableReduction) {
        // Calls the Mutant Reducer
        var (reducedMutants, removedMutantsList) = mutantReducer.reduceMutantsList(mutants, config.reductionRulesList)
        // Checks whether have any mutant removed that should be returned to the main list (to force the execution of a mutant without having to disable the reduction)
        if (config.isForceExecutionEnabled) {
          // List of mutants (Ids) to return to the main list, including the ones that already were forced to run and were killed
          val backMutants = removedMutantsList.filter(m => config.forceExecution.contains(m.mutant.id) || config.killedForceExecutionMutants.contains(m.mutant.id)).map(m => m.mutant)
          reducedMutants = reducedMutants ++ backMutants
          // Updates the list of removed mutants
          removedMutantsList = removedMutantsList.filter(m => !(config.forceExecution.contains(m.mutant.id) || config.killedForceExecutionMutants.contains(m.mutant.id)))
        }
        mutants = reducedMutants
        removedMutants = removedMutants ++ removedMutantsList
      }

      metaMutantBuilder.buildMetaMutantProgramSourceFromMutantProgramSources(programSource, mutants)
    }
    metaMutants.foreach(metaMutant => metaMutantBuilder.writeMetaMutantToFile(metaMutant))
    reporter.reportMutantGenerationEnd(metaMutants, removedMutants)

    // Original Program and Mutants Execution
    reporter.reportMutantExecutionStart
    metaMutantsTestResults = metaMutants.map(metaMutant => mutantRunner.runMutationTestProcess(metaMutant, config.equivalentMutants, config.isTestOnlyLivingMutants, config.livingMutants, config.isForceExecutionEnabled, config.remainingForceExecution)) // the last parameter is the list of remaining mutants of the force execution list to avoid run them again
    metaMutantsVerdicts = metaMutantsTestResults.map(m => MutantAnalyzer.analyzeMutants(m._1, m._2, config.equivalentMutants))
    reporter.reportMutantExecutionEnd(metaMutantsVerdicts)

    reporter.reportProcessEnd
    posProcess()
  }

}