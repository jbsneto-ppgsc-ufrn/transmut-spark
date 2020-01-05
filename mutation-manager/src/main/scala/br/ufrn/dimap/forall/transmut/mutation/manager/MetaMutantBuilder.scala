package br.ufrn.dimap.forall.transmut.mutation.manager

import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.model.Program
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgram
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgram

trait MetaMutantBuilder {

  def buildMetaMutantProgramSourceFromMutantProgramSources(programSource: ProgramSource, mutants: List[MutantProgramSource]): MetaMutantProgramSource

  def buildMetaMutantProgramFromMutantPrograms(program: Program, mutants: List[MutantProgram]): MetaMutantProgram
  
}