package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantKilled
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantError
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantSurvived
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantEquivalent
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgram

case class MetaMutantProgramSourceMetrics(metaMutant: MetaMutantProgramSource, mutantsVerdicts: List[MutantResult[MutantProgramSource]]) {

  def id = metaMutant.id

  def sourceName = metaMutant.original.source.getFileName.toString()

  def originalProgramSourceMetrics = ProgramSourceMetrics(metaMutant.original)

  def metaMutantProgramsMetrics = metaMutant.metaMutantPrograms.map(m => MetaMutantProgramMetrics(m, mutantProgramVerdicts.filter(mt => mt.mutant.original.id == m.id)))

  def mutantProgramVerdicts: List[MutantResult[MutantProgram]] = mutantsVerdicts.map(r => r match {
    case MutantSurvived(m)   => MutantSurvived(m.mutantProgram)
    case MutantEquivalent(m) => MutantEquivalent(m.mutantProgram)
    case MutantKilled(m)     => MutantKilled(m.mutantProgram)
    case MutantError(m)      => MutantError(m.mutantProgram)
  })

  def programs = metaMutant.original.programs

  def totalPrograms = metaMutant.original.programs.size

  def mutants = metaMutant.mutants

  def totalMutants = mutants.size

  def killedMutants = mutantsVerdicts.filter(r => r match {
    case MutantKilled(m) => true
    case _               => false
  }).map(mr => mr.mutant)

  def totalKilledMutants = killedMutants.size

  def survivedMutants = mutantsVerdicts.filter(r => r match {
    case MutantSurvived(m) => true
    case _                 => false
  }).map(mr => mr.mutant)

  def totalSurvivedMutants = survivedMutants.size

  def equivalentMutants = mutantsVerdicts.filter(r => r match {
    case MutantEquivalent(m) => true
    case _                   => false
  }).map(mr => mr.mutant)

  def totalEquivalentMutants = equivalentMutants.size

  def errorMutants = mutantsVerdicts.filter(r => r match {
    case MutantError(m) => true
    case _              => false
  }).map(mr => mr.mutant)

  def totalErrorMutants = errorMutants.size

  def mutationScore = totalKilledMutants.toFloat / (totalMutants - totalEquivalentMutants)

  def numMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (op, metaMutant.mutants.filter(m => m.mutationOperator == op).size)).toMap
}