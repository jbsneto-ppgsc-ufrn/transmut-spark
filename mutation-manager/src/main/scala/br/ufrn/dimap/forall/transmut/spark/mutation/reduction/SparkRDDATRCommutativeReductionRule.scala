package br.ufrn.dimap.forall.transmut.spark.mutation.reduction

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.ATR
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRule
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum.ATRC
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum.ReductionRulesEnum

/**
 * Reduction rule that removes the commutative ATR mutants.
 */
object SparkRDDATRCommutativeReductionRule extends ReductionRule {
  
  def reductionRuleType: ReductionRulesEnum = ATRC

  def isReducible(mutants: List[MutantProgramSource]): Boolean = {
    mutants.exists(m => m.mutationOperator == ATR)
  }

  def reduceMutants(mutants: List[MutantProgramSource]): (List[MutantProgramSource], List[MutantRemoved]) = {
    if (isReducible(mutants)) {
      val newMutants = scala.collection.mutable.ListBuffer.empty[MutantProgramSource]
      val removedMutants = scala.collection.mutable.ListBuffer.empty[MutantRemoved]
      mutants.foreach { m =>
        if (m.mutationOperator == ATR) {
          val mutated = m.mutantProgram.mutantTransformations.asInstanceOf[MutantTransformation].mutated
          if (mutated.name.contains("commutativeReplacement")) {
            removedMutants += MutantRemoved(m, reductionRuleType)
          } else {
            newMutants += m
          }
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