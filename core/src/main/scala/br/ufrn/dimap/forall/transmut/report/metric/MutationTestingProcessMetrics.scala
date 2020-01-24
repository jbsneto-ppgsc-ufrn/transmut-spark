package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantKilled
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantSurvived
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantEquivalent
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantError

case class MutationTestingProcessMetrics(metaMutant: MetaMutantProgramSource, mutantsVerdicts: List[MutantResult[MutantProgramSource]]) {

  def metaMutantMetrics = MetaMutantProgramSourceMetrics(metaMutant)

  def mutationScore = numKilledMutants.toFloat / (metaMutantMetrics.numMutants - numEquivalentMutants)

  def killedMutants = mutantsVerdicts.filter(r => r match {
    case MutantKilled(m) => true
    case _               => false
  }).map(mr => mr.mutant)

  def numKilledMutants = killedMutants.size

  def survivedMutants = mutantsVerdicts.filter(r => r match {
    case MutantSurvived(m) => true
    case _                 => false
  }).map(mr => mr.mutant)

  def numSurvivedMutants = survivedMutants.size

  def equivalentMutants = mutantsVerdicts.filter(r => r match {
    case MutantEquivalent(m) => true
    case _                   => false
  }).map(mr => mr.mutant)

  def numEquivalentMutants = equivalentMutants.size

  def errorMutants = mutantsVerdicts.filter(r => r match {
    case MutantError(m) => true
    case _              => false
  }).map(mr => mr.mutant)

  def numErrorMutants = errorMutants.size

}