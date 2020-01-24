package br.ufrn.dimap.forall.transmut.mutation.analyzer

import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.runner.TestFailed
import br.ufrn.dimap.forall.transmut.mutation.runner.TestResult
import br.ufrn.dimap.forall.transmut.mutation.runner.TestSuccess

object MutantAnalyzer {

  def analyzeMutants(metaMutant: MetaMutantProgramSource, mutantsResults: List[TestResult[MutantProgramSource]]): (MetaMutantProgramSource, List[MutantResult[MutantProgramSource]]) = analyzeMutants(metaMutant, mutantsResults, List())

  def analyzeMutants(metaMutant: MetaMutantProgramSource, mutantsResults: List[TestResult[MutantProgramSource]], equivalentsIds: List[Long]): (MetaMutantProgramSource, List[MutantResult[MutantProgramSource]]) = {
    val mutantsVerdicts: List[MutantResult[MutantProgramSource]] = mutantsResults.map(testResult => fromTestResultToMutantResult(testResult, equivalentsIds))
    (metaMutant, mutantsVerdicts)
  }

  def fromTestResultToMutantResult(testResult: TestResult[MutantProgramSource], equivalentsIds: List[Long]): MutantResult[MutantProgramSource] = {
    if (equivalentsIds.contains(testResult.element.id)) {
      MutantEquivalent(testResult.element)
    } else {
      testResult match {
        case TestSuccess(element) => MutantSurvived(element)
        case TestFailed(element)  => MutantKilled(element)
        case _                    => MutantError(testResult.element)
      }
    }
  }

}