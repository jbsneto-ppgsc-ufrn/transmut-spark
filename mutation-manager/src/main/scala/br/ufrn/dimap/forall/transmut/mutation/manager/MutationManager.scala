package br.ufrn.dimap.forall.transmut.mutation.manager

import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.model.Program
import br.ufrn.dimap.forall.transmut.mutation.model.Mutant
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgram
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource

trait MutationManager {
  
  def generateMutantsFromProgramSource(programSource: ProgramSource, mutationOperators: List[MutationOperatorsEnum]): List[MutantProgramSource]
  
  def generateMutantsFromProgram(program: Program, mutationOperators: List[MutationOperatorsEnum]): List[MutantProgram]
  
}