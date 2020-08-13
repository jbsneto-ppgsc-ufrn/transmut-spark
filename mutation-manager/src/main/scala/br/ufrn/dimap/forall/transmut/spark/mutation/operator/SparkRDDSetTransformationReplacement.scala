package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDBinaryTransformation
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import scala.meta._

object SparkRDDSetTransformationReplacement extends MutationOperator[Transformation] {

  def mutationOperatorType = MutationOperatorsEnum.STR

  def isApplicable(element: Transformation): Boolean = element.isInstanceOf[SparkRDDBinaryTransformation] && (element.name == "union" || element.name == "intersection" || element.name == "subtract")

  def generateMutants(element: Transformation, idGen: LongIdGenerator) = {
    if (isApplicable(element)) {
      val original = element.asInstanceOf[SparkRDDBinaryTransformation]

      original.name match {
        case "union"        => List(intersectionMutant(original, idGen), subtractMutant(original, idGen), firstDatasetMutant(original, idGen), secondDatasetMutant(original, idGen))
        case "intersection" => List(unionMutant(original, idGen), subtractMutant(original, idGen), firstDatasetMutant(original, idGen), secondDatasetMutant(original, idGen))
        case "subtract"     => List(unionMutant(original, idGen), intersectionMutant(original, idGen), firstDatasetMutant(original, idGen), secondDatasetMutant(original, idGen), commutativeMutant(original, idGen))
        case _              => throw new Exception("Not Supported Transformation")
      }
    } else {
      Nil
    }
  }

  private def unionMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "union"
    mutated.source = mutated.source.transform {
      case q"$firstDataset.$setTransformation($secondDataset)" => q"$firstDataset.union($secondDataset)"
    }
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def intersectionMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "intersection"
    mutated.source = mutated.source.transform {
      case q"$firstDataset.$setTransformation($secondDataset)" => q"$firstDataset.intersection($secondDataset)"
    }
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def subtractMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "subtract"
    mutated.source = mutated.source.transform {
      case q"$firstDataset.$setTransformation($secondDataset)" => q"$firstDataset.subtract($secondDataset)"
    }
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def firstDatasetMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "firstDataset"
    mutated.source = mutated.source.transform {
      case q"$firstDataset.$setTransformation($secondDataset)" => q"$firstDataset"
    }
    mutated.params = Nil
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def secondDatasetMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "secondDataset"
    mutated.source = mutated.source.transform {
      case q"$firstDataset.$setTransformation($secondDataset)" => q"$secondDataset"
    }
    mutated.params = Nil
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def commutativeMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = original.name + "Commutative"
    mutated.source = mutated.source.transform {
      case q"$firstDataset.$setTransformation($secondDataset)" => q"$secondDataset.$setTransformation($firstDataset)"
    }

    mutated.source.traverse {
      case q"$firstDataset.$setTransformation(..$params)" => {
        mutated.params = params
      }
      case _ => {}
    }
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

}