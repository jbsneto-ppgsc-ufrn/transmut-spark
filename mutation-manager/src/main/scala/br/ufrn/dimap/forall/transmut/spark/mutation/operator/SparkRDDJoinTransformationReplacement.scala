package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.model.BaseTypesEnum
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDBinaryTransformation
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator

object SparkRDDJoinTransformationReplacement extends MutationOperator[Transformation] {

  def mutationOperatorType = MutationOperatorsEnum.JTR

  def isApplicable(element: Transformation): Boolean = element.isInstanceOf[SparkRDDBinaryTransformation] && (element.name == "join" || element.name == "leftOuterJoin" || element.name == "rightOuterJoin" || element.name == "fullOuterJoin")

  def generateMutants(element: Transformation, idGen: LongIdGenerator) = {
    if (isApplicable(element)) {
      val original = element.asInstanceOf[SparkRDDBinaryTransformation]

      original.name match {
        case "join" => List(joinToLeftOuterJoinMutant(original, idGen), joinToRightOuterJoinMutant(original, idGen), joinToFullOuterJoinMutant(original, idGen))
        case "leftOuterJoin" => List(leftOuterJoinToJoinMutant(original, idGen), leftOuterJoinToRightOuterJoinMutant(original, idGen), leftOuterJoinToFullOuterJoinMutant(original, idGen))
        case "rightOuterJoin" => List(rightOuterJoinToJoinMutant(original, idGen), rightOuterJoinToLeftOuterJoinMutant(original, idGen), rightOuterJoinToFullOuterJoinMutant(original, idGen))
        case "fullOuterJoin" => List(fullOuterJoinToJoinMutant(original, idGen), fullOuterJoinToLeftOuterJoinMutant(original, idGen), fullOuterJoinToRightOuterJoinMutant(original, idGen))
        case _      => throw new Exception("Not Supported Transformation")
      }
    } else {
      Nil
    }
  }

  private def joinToLeftOuterJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "leftOuterJoin"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.join($secondDataset)" => q"$firstDataset.leftOuterJoin($secondDataset).map(tuple => (tuple._1, (tuple._2._1, tuple._2._2.getOrElse($secondRDDDefaultValue))))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def joinToRightOuterJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "rightOuterJoin"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.join($secondDataset)" => q"$firstDataset.rightOuterJoin($secondDataset).map(tuple => (tuple._1, (tuple._2._1.getOrElse($firstRDDDefaultValue), tuple._2._2)))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def joinToFullOuterJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "fullOuterJoin"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.join($secondDataset)" => q"$firstDataset.fullOuterJoin($secondDataset).map(tuple => (tuple._1, (tuple._2._1.getOrElse($firstRDDDefaultValue), tuple._2._2.getOrElse($secondRDDDefaultValue))))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def leftOuterJoinToJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "join"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.leftOuterJoin($secondDataset)" => q"$firstDataset.join($secondDataset).map(tuple => (tuple._1, (tuple._2._1, Option(tuple._2._2))))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def leftOuterJoinToRightOuterJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "rightOuterJoin"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.leftOuterJoin($secondDataset)" => q"$firstDataset.rightOuterJoin($secondDataset).map(tuple => (tuple._1, (tuple._2._1.getOrElse($firstRDDDefaultValue), Option(tuple._2._2))))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def leftOuterJoinToFullOuterJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "fullOuterJoin"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.leftOuterJoin($secondDataset)" => q"$firstDataset.fullOuterJoin($secondDataset).map(tuple => (tuple._1, (tuple._2._1.getOrElse($firstRDDDefaultValue), tuple._2._2)))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def rightOuterJoinToJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "join"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.rightOuterJoin($secondDataset)" => q"$firstDataset.join($secondDataset).map(tuple => (tuple._1, (Option(tuple._2._1), tuple._2._2)))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def rightOuterJoinToLeftOuterJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "leftOuterJoin"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.rightOuterJoin($secondDataset)" => q"$firstDataset.leftOuterJoin($secondDataset).map(tuple => (tuple._1, (Option(tuple._2._1), tuple._2._2.getOrElse($secondRDDDefaultValue))))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def rightOuterJoinToFullOuterJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "fullOuterJoin"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.rightOuterJoin($secondDataset)" => q"$firstDataset.fullOuterJoin($secondDataset).map(tuple => (tuple._1, (tuple._2._1, tuple._2._2.getOrElse($secondRDDDefaultValue))))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def fullOuterJoinToJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "join"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.fullOuterJoin($secondDataset)" => q"$firstDataset.join($secondDataset).map(tuple => (tuple._1, (Option(tuple._2._1), Option(tuple._2._2))))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def fullOuterJoinToLeftOuterJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "leftOuterJoin"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.fullOuterJoin($secondDataset)" => q"$firstDataset.leftOuterJoin($secondDataset).map(tuple => (tuple._1, (Option(tuple._2._1), tuple._2._2)))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }
  
  private def fullOuterJoinToRightOuterJoinMutant(original: SparkRDDBinaryTransformation, idGenerator: LongIdGenerator): MutantTransformation = {
    val mutated = original.copy()
    mutated.name = "rightOuterJoin"
    val firstRDDValueType = getValueTypeFromKeyValueRDD(original.firstInputDataset.get)
    val secondRDDValueType = getValueTypeFromKeyValueRDD(original.secondInputDataset.get)
    val firstRDDDefaultValue = defaultValueFromType(firstRDDValueType)
    val secondRDDDefaultValue = defaultValueFromType(secondRDDValueType)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"$firstDataset.fullOuterJoin($secondDataset)" => q"$firstDataset.rightOuterJoin($secondDataset).map(tuple => (tuple._1, (tuple._2._1, Option(tuple._2._2))))"
        case node                                  => super.apply(node)
      }
    }
    mutated.source = transformer(mutated.source)
    MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
  }

  private def getValueTypeFromKeyValueRDD(dataset: Dataset): br.ufrn.dimap.forall.transmut.model.Type = {
    dataset.datasetType match {
      case ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(key, value))) => value
      case _ => ErrorType()
    }
  }

  private def defaultValueFromType(typ: br.ufrn.dimap.forall.transmut.model.Type): Term = {
    typ match {
      case BaseType(BaseTypesEnum.Int)     => "0".parse[Term].get
      case BaseType(BaseTypesEnum.Long)    => "0l".parse[Term].get
      case BaseType(BaseTypesEnum.Float)   => "0f".parse[Term].get
      case BaseType(BaseTypesEnum.Double)  => "0d".parse[Term].get
      case BaseType(BaseTypesEnum.Boolean) => "false".parse[Term].get
      case BaseType(BaseTypesEnum.Char)    => "\'0\'".parse[Term].get
      case BaseType(BaseTypesEnum.String)  => "\"\"".parse[Term].get
      case ParameterizedType("Option", _)  => "None".parse[Term].get
      case ParameterizedType("List", _)    => "List()".parse[Term].get
      case ParameterizedType("Array", _)   => "Array()".parse[Term].get
      case _                               => "null".parse[Term].get
    }
  }

}