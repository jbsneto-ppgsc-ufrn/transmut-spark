package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._

import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDUnaryTransformation
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.model.Transformation

object SparkRDDFilterTransformationDeletion extends MutationOperator[Transformation] {

  def mutationOperatorType = MutationOperatorsEnum.FTD

  def isApplicable(element: Transformation): Boolean = element.isInstanceOf[SparkRDDUnaryTransformation] && element.name == "filter"

  def generateMutants(element: Transformation, idGenerator : LongIdGenerator) = {
    if(isApplicable(element)){
      val original = element.asInstanceOf[SparkRDDUnaryTransformation]
      val mutated = original.copy()
      mutated.name = "identity"
      mutated.source = mutated.source.transform {
        case q"$dset.filter(..$pars)" => q"$dset"
      }
      mutated.params = Nil
      List(MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType))
    } else {
      Nil
    }
  }

}