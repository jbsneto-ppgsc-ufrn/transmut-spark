package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantKilled
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantSurvived
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantEquivalent
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantError
import scala.concurrent.duration.Duration
import java.time.LocalDateTime

case class MutationTestingProcessMetrics(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])], processDuration: Duration, processStartDateTime: LocalDateTime) {

  def programSources = metaMutantsVerdicts.map(m => m._1.original)

  def metaMutantProgramSources = metaMutantsVerdicts.map(m => m._1)

  def mutantProgramSourcesResults = metaMutantsVerdicts.flatMap(m => m._2)

  def totalMutants = mutantProgramSourcesResults.size
  
  def metaMutantProgramSourcesMetrics = metaMutantsVerdicts.map(mv => MetaMutantProgramSourceMetrics(mv._1, mv._2))
  
  def totalMetaMutanProgramSources = metaMutantProgramSourcesMetrics.size
  
  def metaMutantProgramsMetrics = metaMutantProgramSourcesMetrics.flatMap(m => m.metaMutantProgramsMetrics)
  
  def totalMetaMutantPrograms = metaMutantProgramsMetrics.size
  
  def totalDatasets = metaMutantProgramsMetrics.map(m => m.totalDatasets).sum
  
  def totalTransformations = metaMutantProgramsMetrics.map(m => m.totalTransformations).sum
  
  def mutantProgramsMetrics = metaMutantProgramsMetrics.flatMap(m => m.mutantsMetrics)
  
  def mutationOperatorsMetrics = MutationOperatorsMetrics(mutantProgramsMetrics)

  def killedMutants = mutantProgramSourcesResults.filter(r => r match {
    case MutantKilled(m) => true
    case _               => false
  }).map(mr => mr.mutant)

  def totalKilledMutants = killedMutants.size

  def survivedMutants = mutantProgramSourcesResults.filter(r => r match {
    case MutantSurvived(m) => true
    case _                 => false
  }).map(mr => mr.mutant)

  def totalSurvivedMutants = survivedMutants.size

  def equivalentMutants = mutantProgramSourcesResults.filter(r => r match {
    case MutantEquivalent(m) => true
    case _                   => false
  }).map(mr => mr.mutant)

  def totalEquivalentMutants = equivalentMutants.size

  def errorMutants = mutantProgramSourcesResults.filter(r => r match {
    case MutantError(m) => true
    case _              => false
  }).map(mr => mr.mutant)

  def totalErrorMutants = errorMutants.size

  def totalMutationScore = totalKilledMutants.toFloat / (totalMutants - totalEquivalentMutants)
  
}