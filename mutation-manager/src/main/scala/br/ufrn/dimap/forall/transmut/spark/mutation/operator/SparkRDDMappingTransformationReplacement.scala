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
import br.ufrn.dimap.forall.transmut.model.ErrorType
import br.ufrn.dimap.forall.transmut.model.ClassType

object SparkRDDMappingTransformationReplacement extends MutationOperator[Transformation] {

  def mutationOperatorType = MutationOperatorsEnum.MTR

  def isApplicable(element: Transformation): Boolean = {
    var applicableTransformation = false
    element.source.traverse {
      case q"$rdd.map($func)" => {
        applicableTransformation = true
      }
      case q"$rdd.flatMap($func)" => {
        applicableTransformation = true
      }
    }
    element.isInstanceOf[SparkRDDUnaryTransformation] && applicableTransformation
  }

  def generateMutants(element: Transformation, idGen: LongIdGenerator) = {
    if (isApplicable(element)) {
      val original = element.asInstanceOf[SparkRDDUnaryTransformation]

      original.name match {
        case "map"     => generateMapMutants(original, idGen)
        case "flatMap" => generateFlatMapMutants(original, idGen)
        case _         => throw new Exception("Not Supported Transformation")
      }
    } else {
      Nil
    }
  }

  def generateMapMutants(original: SparkRDDUnaryTransformation, idGen: LongIdGenerator): List[MutantTransformation] = {
    val outputType = getOutputType(original)
    val inputType = getInputType(original).simplifiedName.parse[Type].get
    val mutants = scala.collection.mutable.ListBuffer.empty[MutantTransformation]
    val mappingValues = mappingValuesFromType(outputType)
    mappingValues.foreach { term =>
      val mutated = original.copy()
      mutated.name = "mapTo" + term.syntax
      val transformer = new Transformer {
        override def apply(tree: Tree): Tree = tree match {
          case q"$rdd.$tranf($func)" => q"""$rdd.$tranf((inputParameter: $inputType) => {
                  val originalFunction = ($func)(_)
                  val originalValue = originalFunction(inputParameter)
                  $term
              })"""
          case node => super.apply(node)
        }
      }
      mutated.source = transformer(mutated.source)
      val traverser = new Traverser {
        override def apply(tree: Tree): Unit = tree match {
          case q"$rdd.$tranf(..$params)" => {
            mutated.params = params
          }
          case node => super.apply(node)
        }
      }
      traverser(mutated.source)
      mutants += MutantTransformation(idGen.getId, original, mutated, mutationOperatorType)
    }
    mutants.toList
  }

  private def mappingValuesFromType(typ: br.ufrn.dimap.forall.transmut.model.Type): List[Term] = {
    typ match {
      case BaseType(BaseTypesEnum.Int)     => List("0", "1", "Int.MaxValue", "Int.MinValue", "-originalValue").map(t => t.parse[Term].get)
      case BaseType(BaseTypesEnum.Long)    => List("0l", "1l", "Long.MaxValue", "Long.MinValue", "-originalValue").map(t => t.parse[Term].get)
      case BaseType(BaseTypesEnum.Float)   => List("0f", "1f", "Float.MaxValue", "Float.MinValue", "-originalValue").map(t => t.parse[Term].get)
      case BaseType(BaseTypesEnum.Double)  => List("0d", "1d", "Double.MaxValue", "Double.MinValue", "-originalValue").map(t => t.parse[Term].get)
      case BaseType(BaseTypesEnum.Boolean) => List("false", "true", "!originalValue").map(t => t.parse[Term].get)
      case BaseType(BaseTypesEnum.Char)    => List("0.toChar", "1.toChar", "Char.MaxValue", "Char.MinValue", "-originalValue").map(t => t.parse[Term].get)
      case BaseType(BaseTypesEnum.String)  => List("\"\"".parse[Term].get)
      case ParameterizedType(name, List(typeParam)) if name.replace('/', '.').split('.').last.replace("#", "") == "Option" => {
        val typeName = typeParam.simplifiedName
        List(s"List[$typeName]().headOption".parse[Term].get) // List[$typeName]().headOption = None (to force Option[$typeName])
      }
      case ParameterizedType(name, List(typeParam)) if name.replace('/', '.').split('.').last.replace("#", "") == "List" => {
        val typeName = typeParam.simplifiedName
        List(s"List[$typeName](originalValue.head)", "originalValue.tail", "originalValue.reverse", s"List[$typeName]()").map(t => t.parse[Term].get)
      }
      case ParameterizedType(name, List(typeParam)) if name.replace('/', '.').split('.').last.replace("#", "") == "Array" => {
        val typeName = typeParam.simplifiedName
        List(s"Array[$typeName](originalValue.head)", "originalValue.tail", "originalValue.reverse", s"Array[$typeName]()").map(t => t.parse[Term].get)
      }
      case ParameterizedType(name, List(typeParam)) if name.replace('/', '.').split('.').last.replace("#", "") == "Set" => {
        val typeName = typeParam.simplifiedName
        List(s"Set[$typeName](originalValue.head)", "originalValue.tail", s"Set[$typeName]()").map(t => t.parse[Term].get)
      }
      case TupleType(key, value) => mappingValuesFromKeyValue(key, value)
      case classType: ClassType => {
        val typeName = classType.simplifiedName
        List(s"null.asInstanceOf[$typeName]".parse[Term].get)
      }
      case _ => Nil
    }
  }

  private def mappingValuesFromKeyValue(keyType: br.ufrn.dimap.forall.transmut.model.Type, valueType: br.ufrn.dimap.forall.transmut.model.Type): List[Term] = {
    val keyValuesTerms = mappingValuesFromType(keyType)
    val valueValuesTerms = mappingValuesFromType(valueType)
    val keyTerms = keyValuesTerms.map(t => {
      var term = t
      if (term.syntax.contains("originalValue")) {
        term = term.syntax.replaceAll("originalValue", "originalValue._1").parse[Term].get
      }
      q"($term, originalValue._2)"
    })
    val valueTerms = valueValuesTerms.map(t => {
      var term = t
      if (term.syntax.contains("originalValue")) {
        term = term.syntax.replaceAll("originalValue", "originalValue._2").parse[Term].get
      }
      q"(originalValue._1, $term)"
    })
    keyTerms ++ valueTerms
  }

  def generateFlatMapMutants(original: SparkRDDUnaryTransformation, idGenerator: LongIdGenerator): List[MutantTransformation] = {
    val inputType = getInputType(original).simplifiedName.parse[Type].get
    val outputType = getOutputType(original).simplifiedName
    val mappingValues = List("originalValue.headOption", "originalValue.toList.tail", "originalValue.toList.reverse", s"List[$outputType]()").map(t => t.parse[Term].get)
    val mutants = scala.collection.mutable.ListBuffer.empty[MutantTransformation]
    mappingValues.foreach { term =>
      val mutated = original.copy()
      mutated.name = "flatMapTo" + term.syntax
      val inputType = getInputType(original).simplifiedName.parse[Type].get
      val transformer = new Transformer {
        override def apply(tree: Tree): Tree = tree match {
          case q"$rdd.$tranf($func)" => q"""$rdd.$tranf((inputParameter: $inputType) => {
              val originalFunction = ($func)(_)
              val originalValue = originalFunction(inputParameter)
              $term
           })"""
          case node => super.apply(node)
        }
      }
      mutated.source = transformer(mutated.source)
      val traverser = new Traverser {
        override def apply(tree: Tree): Unit = tree match {
          case q"$rdd.$tranf(..$params)" => {
            mutated.params = params
          }
          case node => super.apply(node)
        }
      }
      traverser(mutated.source)
      mutants += MutantTransformation(idGenerator.getId, original, mutated, mutationOperatorType)
    }
    mutants.toList
  }

  private def getOutputType(transformation: SparkRDDUnaryTransformation): br.ufrn.dimap.forall.transmut.model.Type = {
    val outputType = transformation.outputTypes.head
    outputType match {
      case ParameterizedType("org/apache/spark/rdd/RDD#", List(valueType)) => valueType
      case _ => ErrorType()
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