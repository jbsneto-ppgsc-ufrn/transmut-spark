package br.ufrn.dimap.forall.transmut.spark.mutation.reduction

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.DTI
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRule
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum._
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved

/**
 * Reduction rule that removes obvious DTI equivalent mutants, which are the distinct transformations that appear right after grouping or aggregation transformations.
 */
object SparkRDDDTIEquivalentReductionRule extends ReductionRule {

  val transformations = Set("reduceByKey", "combineByKey", "aggregateByKe", "foldByKey", "groupByKey", "groupBy", "groupWith")
  
  def reductionRuleType: ReductionRulesEnum = DTIE

  def isReducible(mutants: List[MutantProgramSource]): Boolean = {
    mutants.exists(m => m.mutationOperator == DTI &&
      transformations.contains(m.mutantProgram.mutantTransformations.asInstanceOf[MutantTransformation].original.name))
  }

  def reduceMutants(mutants: List[MutantProgramSource]): (List[MutantProgramSource], List[MutantRemoved]) = {
    if (isReducible(mutants)) {
      val newMutants = scala.collection.mutable.ListBuffer.empty[MutantProgramSource]
      val removedMutants = scala.collection.mutable.ListBuffer.empty[MutantRemoved]
      mutants.foreach { m =>
        if (m.mutationOperator == DTI) {
          val original = m.mutantProgram.mutantTransformations.asInstanceOf[MutantTransformation].original
          if (transformations.contains(original.name)) {
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