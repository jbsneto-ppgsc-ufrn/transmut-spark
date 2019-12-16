package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._

import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDUnaryTransformation
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.model.ParameterizedType
import br.ufrn.dimap.forall.transmut.model.BaseTypesEnum
import br.ufrn.dimap.forall.transmut.model.TupleType
import br.ufrn.dimap.forall.transmut.model.BaseType

object SparkRDDAggregationTransformationReplacement extends MutationOperator[Transformation] {

  def mutationOperatorType = MutationOperatorsEnum.ATR

  def isApplicable(element: Transformation): Boolean = {
    var applicableTransformation = false
    element.source.traverse {
      case q"$rdd.reduceByKey($func)" => {
        applicableTransformation = true
      }
      case q"$rdd.combineByKey($firstParam, $secondParam, $func)" => {
        applicableTransformation = true
      }
    }
    element.isInstanceOf[SparkRDDUnaryTransformation] && applicableTransformation
  }

  def generateMutants(element: Transformation, idGen: LongIdGenerator) = {
    if (isApplicable(element)) {
      val original = element.asInstanceOf[SparkRDDUnaryTransformation]

      original.name match {
        case "reduceByKey"  => List(firstParameterMutant(original, idGen), secondParameterMutant(original, idGen), firstParameterReplacementMutant(original, idGen), secondParameterReplacementMutant(original, idGen), commutativeReplacementMutant(original, idGen))
        case "combineByKey" => List(firstParameterMutant(original, idGen), secondParameterMutant(original, idGen), firstParameterReplacementMutant(original, idGen), secondParameterReplacementMutant(original, idGen), commutativeReplacementMutant(original, idGen))
        case _              => throw new Exception("Not Supported Transformation")
      }
    } else {
      Nil
    }
  }

  private def firstParameterMutant(original: SparkRDDUnaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "firstParameter"
    val paramType = getFunctionParameterTypeName(original).parse[Type].get
    mutated.source = mutated.source.transform {
      case q"$rdd.$tranf($func)"                            => q"""$rdd.$tranf((firstParameter: $paramType, secondParameter: $paramType) => firstParameter)"""
      case q"$rdd.$tranf($firstParam, $secondParam, $func)" => q"""$rdd.$tranf($firstParam, $secondParam, (firstParameter: $paramType, secondParameter: $paramType) => firstParameter)"""
    }

    mutated.source.traverse {
      case q"$rdd.$tranf(..$params)" => {
        mutated.params = params
      }
      case _ => {}
    }

    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def secondParameterMutant(original: SparkRDDUnaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "secondParameter"
    val paramType = getFunctionParameterTypeName(original).parse[Type].get
    mutated.source = mutated.source.transform {
      case q"$rdd.$tranf($func)"                            => q"""$rdd.$tranf((firstParameter: $paramType, secondParameter: $paramType) => secondParameter)"""
      case q"$rdd.$tranf($firstParam, $secondParam, $func)" => q"""$rdd.$tranf($firstParam, $secondParam, (firstParameter: $paramType, secondParameter: $paramType) => secondParameter)"""
    }

    mutated.source.traverse {
      case q"$rdd.$tranf(..$params)" => {
        mutated.params = params
      }
      case _ => {}
    }

    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def firstParameterReplacementMutant(original: SparkRDDUnaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "firstParameterReplacement"
    val paramType = getFunctionParameterTypeName(original).parse[Type].get
    mutated.source = mutated.source.transform {
      case q"$rdd.$tranf($func)" => q"""$rdd.$tranf((firstParameter: $paramType, secondParameter: $paramType) => {
      val originalFunction = $func
      originalFunction(firstParameter, firstParameter)
      })"""
      case q"$rdd.$tranf($firstParam, $secondParam, $func)" => q"""$rdd.$tranf($firstParam, $secondParam, (firstParameter: $paramType, secondParameter: $paramType) => {
      val originalFunction = $func
      originalFunction(firstParameter, firstParameter)
      })"""
    }
    mutated.source.traverse {
      case q"$rdd.$tranf(..$params)" => {
        mutated.params = params
      }
      case _ => {}
    }

    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def secondParameterReplacementMutant(original: SparkRDDUnaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "secondParameterReplacement"
    val paramType = getFunctionParameterTypeName(original).parse[Type].get
    mutated.source = mutated.source.transform {
      case q"$rdd.$tranf($func)" => q"""$rdd.$tranf((firstParameter: $paramType, secondParameter: $paramType) => {
      val originalFunction = $func
      originalFunction(secondParameter, secondParameter)
      })"""
      case q"$rdd.$tranf($firstParam, $secondParam, $func)" => q"""$rdd.$tranf($firstParam, $secondParam, (firstParameter: $paramType, secondParameter: $paramType) => {
      val originalFunction = $func
      originalFunction(secondParameter, secondParameter)
      })"""
    }

    mutated.source.traverse {
      case q"$rdd.$tranf(..$params)" => {
        mutated.params = params
      }
      case _ => {}
    }

    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def commutativeReplacementMutant(original: SparkRDDUnaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "commutativeReplacement"
    val paramType = getFunctionParameterTypeName(original).parse[Type].get
    mutated.source = mutated.source.transform {
      case q"$rdd.$tranf($func)" => q"""$rdd.$tranf((firstParameter: $paramType, secondParameter: $paramType) => {
      val originalFunction = $func
      originalFunction(secondParameter, firstParameter)
      })"""
      case q"$rdd.$tranf($firstParam, $secondParam, $func)" => q"""$rdd.$tranf($firstParam, $secondParam, (firstParameter: $paramType, secondParameter: $paramType) => {
      val originalFunction = $func
      originalFunction(secondParameter, firstParameter)
      })"""
    }

    mutated.source.traverse {
      case q"$rdd.$tranf(..$params)" => {
        mutated.params = params
      }
      case _ => {}
    }

    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def getFunctionParameterTypeName(transformation: SparkRDDUnaryTransformation): String = {
    val outputType = transformation.outputTypes.head
    outputType match {
      case ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(key, value))) => value.simplifiedName
      case ParameterizedType("org/apache/spark/rdd/RDD#", List(otherType)) => otherType.simplifiedName
      case _ => ""
    }
  }

}