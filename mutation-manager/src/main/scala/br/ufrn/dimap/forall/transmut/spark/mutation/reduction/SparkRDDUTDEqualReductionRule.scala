package br.ufrn.dimap.forall.transmut.spark.mutation.reduction

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.DTD
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.FTD
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.OTD
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.UTD
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRule
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum.ReductionRulesEnum
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum.UTDE

/**
 * Reduction rule that removes FTD, OTD and DTD mutants if there are UTD mutants.
 */
object SparkRDDUTDEqualReductionRule extends ReductionRule {
  
  def reductionRuleType: ReductionRulesEnum = UTDE

  def isReducible(mutants: List[MutantProgramSource]): Boolean = {
    mutants.exists(m => m.mutationOperator == UTD) && mutants.exists(m => m.mutationOperator == FTD || m.mutationOperator == OTD || m.mutationOperator == DTD)
  }

  def reduceMutants(mutants: List[MutantProgramSource]): (List[MutantProgramSource], List[MutantRemoved]) = {
    if (isReducible(mutants)) {
      val newMutants = scala.collection.mutable.ListBuffer.empty[MutantProgramSource]
      val removedMutants = scala.collection.mutable.ListBuffer.empty[MutantRemoved]
      mutants.foreach { m =>
        if (m.mutationOperator == FTD || m.mutationOperator == OTD || m.mutationOperator == DTD) {
          removedMutants += MutantRemoved(m, reductionRuleType)
        } else {
          newMutants += m
        }
      }
      (newMutants.toList, removedMutants.toList)
    } else {
      (mutants, Nil)
    }
  }

}