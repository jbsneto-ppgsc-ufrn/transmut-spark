package br.ufrn.dimap.forall.transmut.mutation.runner

import br.ufrn.dimap.forall.transmut.mutation.model.Mutant
import br.ufrn.dimap.forall.transmut.model.Program
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.exception.OriginalTestExecutionException

trait MutantRunner {

  def runMutationTestProcess(metaMutant: MetaMutantProgramSource, equivalentMutants: List[Long], testOnlyLivingMutants: Boolean = false, livingMutants: List[Long] = List()): (MetaMutantProgramSource, List[TestResult[MutantProgramSource]]) = {
    val originalResult = runOriginalTest(metaMutant.original)
    val list = originalResult match {
      case TestSuccess(_) => metaMutant.mutants.map(mutant => runMutantTest(mutant, equivalentMutants, testOnlyLivingMutants, livingMutants))
      case _              => throw new OriginalTestExecutionException("The original program tests failed, fix them and run the mutation testisng process again!")
    }
    (metaMutant, list)
  }

  def runOriginalTest(programSource: ProgramSource): TestResult[ProgramSource]

  def runMutantTest(mutant: MutantProgramSource, equivalentMutants: List[Long], testOnlyLivingMutants: Boolean = false, livingMutants: List[Long] = List()): TestResult[MutantProgramSource]

}