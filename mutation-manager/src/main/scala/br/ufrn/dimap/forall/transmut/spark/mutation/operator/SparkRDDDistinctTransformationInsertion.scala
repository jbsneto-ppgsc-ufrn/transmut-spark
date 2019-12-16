package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._

import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDTransformation
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator

object SparkRDDDistinctTransformationInsertion extends MutationOperator[Transformation] {

  def mutationOperatorType = MutationOperatorsEnum.DTI

  def isApplicable(element: Transformation): Boolean = element.isInstanceOf[SparkRDDTransformation] && !element.isLoadTransformation && element.name != "distinct"

  def generateMutants(element: Transformation, idGenerator: LongIdGenerator) = {
    if (isApplicable(element)) {
      val original = element.asInstanceOf[SparkRDDTransformation]
      val mutated = original.copy().asInstanceOf[SparkRDDTransformation]
      mutated.name = original.name + "Distinct"
      mutated.source = transformerDistinct(mutated.source)
      List(MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType))
    } else {
      Nil
    }
  }

  val transformerDistinct = new Transformer {
    override def apply(tree: Tree): Tree = tree match {
      case q"$dset.$transf(..$pars)" => q"$dset.$transf(..$pars).distinct()"
      case q"$dset.$transf()"        => q"$dset.$transf().distinct()"
      case q"$dset.$transf"          => q"$dset.$transf.distinct()"
      case node                      => super.apply(node)
    }
  }

}