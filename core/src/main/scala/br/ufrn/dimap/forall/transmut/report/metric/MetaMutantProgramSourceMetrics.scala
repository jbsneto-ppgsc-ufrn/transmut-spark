package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantKilled
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantError
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantLived
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantEquivalent
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgram
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgram

case class MetaMutantProgramSourceMetrics(metaMutant: MetaMutantProgramSource, mutantsVerdicts: List[MutantResult[MutantProgramSource]], removedMutantsList: List[MutantRemoved]) {

  def id = metaMutant.id
  
  def source = metaMutant.original.source

  def sourceName = source.getFileName.toString().replaceFirst(".scala", "")

  def originalProgramSourceMetrics = ProgramSourceMetrics(metaMutant.original)

  def metaMutantProgramsMetrics = metaMutant.metaMutantPrograms.map(m => MetaMutantProgramMetrics(m, mutantProgramVerdicts.filter(mt => mt.mutant.original.id == m.id), removedMutantsFromProgram(m, removedMutantsList)))

  def mutantProgramsMetrics = metaMutantProgramsMetrics.flatMap(m => m.mutantsMetrics)
  
  def removedMutantsMetrics = metaMutantProgramsMetrics.flatMap(m => m.removedMutantsMetrics)
  
  def mutationOperatorsMetrics = MutationOperatorsMetrics(mutantProgramsMetrics, removedMutantsMetrics)
  
  def mutantProgramVerdicts: List[MutantResult[MutantProgram]] = mutantsVerdicts.map(r => r match {
    case MutantLived(m)   => MutantLived(m.mutantProgram)
    case MutantEquivalent(m) => MutantEquivalent(m.mutantProgram)
    case MutantKilled(m)     => MutantKilled(m.mutantProgram)
    case MutantError(m)      => MutantError(m.mutantProgram)
  })

  def programs = metaMutant.original.programs

  def totalPrograms = metaMutant.original.programs.size
  
  def totalDatasets = metaMutantProgramsMetrics.map(m => m.totalDatasets).sum
  
  def totalTransformations = metaMutantProgramsMetrics.map(m => m.totalTransformations).sum

  def mutants = metaMutant.mutants

  def totalMutants = mutants.size + removedMutants.size

  def killedMutants = mutantsVerdicts.filter(r => r match {
    case MutantKilled(m) => true
    case _               => false
  }).map(mr => mr.mutant)

  def totalKilledMutants = killedMutants.size

  def livedMutants = mutantsVerdicts.filter(r => r match {
    case MutantLived(m) => true
    case _                 => false
  }).map(mr => mr.mutant)

  def totalLivedMutants = livedMutants.size

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
  
  def removedMutants = removedMutantsList

  def totalRemovedMutants = removedMutants.size

  def mutationScore = totalKilledMutants.toFloat / (totalMutants - totalEquivalentMutants - totalRemovedMutants)
  
  private def removedMutantsFromProgram(program: MetaMutantProgram, removedMutants: List[MutantRemoved]) = removedMutants.filter(rm => rm.mutant.mutantProgram.original.id == program.original.id)
  
}