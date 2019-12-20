package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._

import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDUnaryTransformation
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.mutation.model.MutantListTransformation

object SparkRDDUnaryTransformationDeletion extends MutationOperator[List[Transformation]] {

  def mutationOperatorType = MutationOperatorsEnum.UTD

  def isApplicable(elements: List[Transformation]): Boolean = {
    for (element <- elements) {
      val isApplicableElement = element.isInstanceOf[SparkRDDUnaryTransformation] && 
                                  element.inputTypes.size == 1 && 
                                  element.outputTypes.size == 1 && 
                                  element.inputTypes.head == element.outputTypes.head
      if (isApplicableElement)
        return true
    }
    return false
  }

  def generateMutants(elements: List[Transformation], idGenerator: LongIdGenerator) = {
    if (isApplicable(elements)) {
      val listTransformations = scala.collection.mutable.ListBuffer.empty[MutantListTransformation]
      for (element <- elements) {
        val isApplicableElement = element.isInstanceOf[SparkRDDUnaryTransformation] && element.inputTypes.size == 1 && element.outputTypes.size == 1 && element.inputTypes.head == element.outputTypes.head
        if (isApplicableElement) {
          val original = element.asInstanceOf[SparkRDDUnaryTransformation]
          val mutated = original.copy()
          mutated.name = original.name + "Deletion"
          mutated.source = mutated.source.transform {
            case q"$dset.$tranfs(..$pars)" => q"$dset"
            case q"$dset.$tranfs()"        => q"$dset"
            case q"$dset.$tranfs"          => q"$dset"
          }
          mutated.params = Nil
          listTransformations += MutantListTransformation(idGenerator.getId, List(original), List(mutated), mutationOperatorType)
        }
      }
      listTransformations.toList
    } else {
      Nil
    }
  }

}