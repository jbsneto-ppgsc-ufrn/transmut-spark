package br.ufrn.dimap.forall.transmut.mutation.manager

import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.model.Program
import br.ufrn.dimap.forall.transmut.mutation.model.Mutant
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgram
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator

trait MutationManager {

  def defaultMutantsIdGenerator = LongIdGenerator.generator

  def generateMutantsFromProgramSource(programSource: ProgramSource, mutationOperators: List[MutationOperatorsEnum], idGenerator: LongIdGenerator): List[MutantProgramSource]

  def generateMutantsFromProgramSource(programSource: ProgramSource, mutationOperators: List[MutationOperatorsEnum]): List[MutantProgramSource] = generateMutantsFromProgramSource(programSource, mutationOperators, defaultMutantsIdGenerator)

  def generateMutantsFromProgram(program: Program, mutationOperators: List[MutationOperatorsEnum], idGenerator: LongIdGenerator): List[MutantProgram]

  def generateMutantsFromProgram(program: Program, mutationOperators: List[MutationOperatorsEnum]): List[MutantProgram] = generateMutantsFromProgram(program, mutationOperators, defaultMutantsIdGenerator)

}