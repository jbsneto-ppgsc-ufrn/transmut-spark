package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._

import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDUnaryTransformation
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.model.ErrorType
import br.ufrn.dimap.forall.transmut.model.ParameterizedType

object SparkRDDNegationFilterTransformationPredicate extends MutationOperator[Transformation] {

  def mutationOperatorType = MutationOperatorsEnum.NFTP

  def isApplicable(element: Transformation): Boolean = element.isInstanceOf[SparkRDDUnaryTransformation] && element.name == "filter"

  def generateMutants(element: Transformation, idGenerator: LongIdGenerator) = {
    if (isApplicable(element)) {
      val original = element.asInstanceOf[SparkRDDUnaryTransformation]
      val inputType = getInputType(original).simplifiedName.parse[Type].get
      val mutated = original.copy()
      mutated.name = "filterNot"
      val transformer = new Transformer {
        override def apply(tree: Tree): Tree = tree match {
          case q"$rdd.filter($func)" => q"""$rdd.filter((inputParameter: $inputType) => {
              val originalFunction = ($func)(_)
              val originalValue = originalFunction(inputParameter)
              !originalValue
           })"""
          case node => super.apply(node)
        }
      }
      mutated.source = transformer(mutated.source)
      val traverser = new Traverser {
        override def apply(tree: Tree): Unit = tree match {
          case q"$rdd.filter(..$params)" => {
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

  private def getInputType(transformation: SparkRDDUnaryTransformation): br.ufrn.dimap.forall.transmut.model.Type = {
    val inputType = transformation.inputTypes.head
    inputType match {
      case ParameterizedType("org/apache/spark/rdd/RDD#", List(valueType)) => valueType
      case _ => ErrorType()
    }
  }

}