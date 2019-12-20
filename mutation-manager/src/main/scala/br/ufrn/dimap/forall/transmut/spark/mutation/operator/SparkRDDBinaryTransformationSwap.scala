package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._

import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDUnaryTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDBinaryTransformation
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.mutation.model.MutantListTransformation

object SparkRDDBinaryTransformationSwap extends MutationOperator[List[Transformation]] {

  def mutationOperatorType = MutationOperatorsEnum.BTS

  def isApplicable(elements: List[Transformation]): Boolean = {
    for (i <- 0 to (elements.size - 1); j <- (i + 1) to (elements.size - 1)) {
      val firstTransformation = elements(i)
      val secondTransformation = elements(j)
      val isApplicableElement = firstTransformation.id != secondTransformation.id &&
        firstTransformation.isInstanceOf[SparkRDDBinaryTransformation] &&
        secondTransformation.isInstanceOf[SparkRDDBinaryTransformation] &&
        firstTransformation.inputTypes == secondTransformation.inputTypes &&
        firstTransformation.outputTypes == secondTransformation.outputTypes &&
        firstTransformation.name != secondTransformation.name
      if (isApplicableElement) {
        return true
      }
    }
    return false
  }

  def generateMutants(elements: List[Transformation], idGenerator: LongIdGenerator) = {
    if (isApplicable(elements)) {
      val listTransformations = scala.collection.mutable.ListBuffer.empty[MutantListTransformation]
      for (i <- 0 to (elements.size - 1); j <- (i + 1) to (elements.size - 1)) {
        val firstTransformation = elements(i)
        val secondTransformation = elements(j)
        val isApplicableElement = firstTransformation.id != secondTransformation.id &&
          firstTransformation.isInstanceOf[SparkRDDBinaryTransformation] &&
          secondTransformation.isInstanceOf[SparkRDDBinaryTransformation] &&
          firstTransformation.inputTypes == secondTransformation.inputTypes &&
          firstTransformation.outputTypes == secondTransformation.outputTypes &&
          firstTransformation.name != secondTransformation.name
        if (isApplicableElement) {
          val original1 = firstTransformation.asInstanceOf[SparkRDDBinaryTransformation]
          val original2 = secondTransformation.asInstanceOf[SparkRDDBinaryTransformation]
          
          // Mutant 1 - Replaces the first transformation with the second
          val mutated1 = original1.copy()
          mutated1.name = original1.name + "To" + original2.name
          // We take the second transformation and its parameters and assign it to the first transformation/mutant (without changing the second transformation)
          // Custom traverser and transformers are necessary to avoid repetition in recursions
          val traverser1 = new Traverser {
            override def apply(tree: Tree): Unit = tree match {
              case q"$dset2.$tranfs2(..$pars2)" => {
                val transformer = new Transformer {
                  override def apply(tree: Tree): Tree = tree match {
                    case q"$dset1.$tranfs1(..$pars1)" => q"$dset1.$tranfs2(..$pars1)"
                    case node                         => super.apply(node)
                  }
                }
                mutated1.source = transformer(mutated1.source)
              }
              case node => super.apply(node)
            }
          }
          traverser1(original2.source)
          
          // Mutant 2 - Replaces the second transformation with the first
          val mutated2 = original2.copy()
          mutated2.name = original2.name + "To" + original1.name
          // We take the second transformation and its parameters and assign it to the first transformation/mutant (without changing the second transformation)
          // Custom traverser and transformers are necessary to avoid repetition in recursions
          val traverser2 = new Traverser {
            override def apply(tree: Tree): Unit = tree match {
              case q"$dset2.$tranfs2(..$pars2)" => {
                val transformer = new Transformer {
                  override def apply(tree: Tree): Tree = tree match {
                    case q"$dset1.$tranfs1(..$pars1)" => q"$dset1.$tranfs2(..$pars1)"
                    case node                         => super.apply(node)
                  }
                }
                mutated2.source = transformer(mutated2.source)
              }
              case node => super.apply(node)
            }
          }
          traverser2(original1.source)
          
          listTransformations += MutantListTransformation(idGenerator.getId, List(original1, original2), List(mutated1, mutated2), mutationOperatorType)
        }
      }
      listTransformations.toList
    } else {
      Nil
    }
  }

}