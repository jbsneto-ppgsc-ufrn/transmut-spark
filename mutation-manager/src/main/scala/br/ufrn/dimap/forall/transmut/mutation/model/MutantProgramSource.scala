package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.model.Program
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._

/**
 * A mutant Program Source that have a mutant program.
 * A mutant program source only exists with a mutant program, so its id and mutarionOperator are the same as the mutant program.
 *
 * @constructor create a new mutant program source with an original program source, mutated program source and a mutant program.
 * @param original program source
 * @param mutated program source
 * @param mutantProgram the mutant program
 */
case class MutantProgramSource(override val original: ProgramSource, override val mutated: ProgramSource, mutantProgram: MutantProgram) extends Mutant[ProgramSource]{
 
  override val id: Long = mutantProgram.id

  override val mutationOperator: MutationOperatorsEnum = mutantProgram.mutationOperator
  
}