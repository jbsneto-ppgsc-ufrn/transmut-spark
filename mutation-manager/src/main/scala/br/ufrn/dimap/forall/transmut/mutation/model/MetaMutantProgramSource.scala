package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.ProgramSource

case class MetaMutantProgramSource(override val original: ProgramSource, override val mutated: ProgramSource, override val mutants: List[MutantProgramSource], val metaMutantPrograms: List[MetaMutantProgram]) extends MetaMutant[ProgramSource] {
  
  // Pre-condition: the number of meta-mutant programs must be equal to the number of programs in the program source
  require(metaMutantPrograms.size == original.programs.size)
  // Pre-condition: the total number of mutant programs from the meta-mutant programs must be equal to the number of mutant program sources
  require(metaMutantPrograms.flatMap(m => m.mutants).size == mutants.size)
  
  override val id: Long = original.id
}