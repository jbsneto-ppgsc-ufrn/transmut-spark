package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._

import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDUnaryTransformation
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.model.Transformation

object SparkRDDOrderTransformationInversion extends MutationOperator[Transformation] {

  def mutationOperatorType = MutationOperatorsEnum.OTI

  def isApplicable(element: Transformation): Boolean = element.isInstanceOf[SparkRDDUnaryTransformation] && (element.name == "sortBy" || element.name == "sortByKey")

  def generateMutants(element: Transformation, idGenerator: LongIdGenerator) = {
    if (isApplicable(element)) {
      val original = element.asInstanceOf[SparkRDDUnaryTransformation]
      val mutated = original.copy()
      mutated.name = original.name + "Inverted"
      val transformer = new Transformer {
        override def apply(tree: Tree): Tree = tree match {
          case q"$dset.sortBy($func)"         => q"$dset.sortBy($func, false)" // is true by default
          case q"$dset.sortBy($func, true)"   => q"$dset.sortBy($func, false)"
          case q"$dset.sortBy($func, false)"  => q"$dset.sortBy($func, true)"
          case q"$dset.sortBy($func, $order)" => q"$dset.sortBy($func, !($order))" // General in case of using some variable or expression
          case q"$dset.sortByKey()"           => q"$dset.sortByKey(false)"
          case q"$dset.sortByKey(true)"       => q"$dset.sortByKey(false)"
          case q"$dset.sortByKey(false)"      => q"$dset.sortByKey(true)"
          case q"$dset.sortByKey($order)"     => q"$dset.sortByKey(!($order))"
          case q"$dset.sortByKey"             => q"$dset.sortByKey(false)"
          case node                           => super.apply(node)
        }
      }
      mutated.source = transformer(mutated.source)
      val traverser = new Traverser {
        override def apply(tree: Tree): Unit = tree match {
          case q"$dset.sortBy(..$params)" => {
            mutated.params = params
          }
          case q"$dset.sortByKey(..$params)" => {
            mutated.params = params
          }
          case node => super.apply(node)
        }
      }
      traverser(mutated.source)
      List(MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType))
    } else {
      Nil
    }
  }

}