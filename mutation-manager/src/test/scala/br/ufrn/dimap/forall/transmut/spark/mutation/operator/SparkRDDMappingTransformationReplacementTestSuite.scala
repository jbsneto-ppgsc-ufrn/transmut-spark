package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._
import scala.meta.Tree
import scala.meta.contrib._

import org.scalatest.FunSuite
import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDUnaryTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDBinaryTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDOperation
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

class SparkRDDMappingTransformationReplacementTestSuite extends FunSuite {

  test("Test Case 1 - Not Applicable Unary Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.filter(a => a > 1)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(!SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)
  }

  test("Test Case 2 - Not Applicable Binary Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.union(rdd2)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(!SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)
  }

  test("Test Case 3 - FlatMap") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.flatMap((x: String) => x.split(" "))
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 4)

    val mutantHead = mutants(0)
    val mutantTail = mutants(1)
    val mutantReverse = mutants(2)
    val mutantEmpty = mutants(3)

    // Head Mutant
    assert(mutantHead.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantHead.original == original)
    assert(mutantHead.mutated != original)

    assert(mutantHead.mutated.id == mutantHead.original.id)
    assert(mutantHead.mutated.edges == mutantHead.original.edges)

    assert(mutantHead.mutated.name != mutantHead.original.name)
    assert(mutantHead.mutated.name == "flatMapTooriginalValue.headOption")

    assert(mutantHead.mutated.source != mutantHead.original.source)
    assert(mutantHead.original.source.isEqual(q"""val rdd2 = rdd1.flatMap((x: String) => x.split(" "))"""))
    assert(mutantHead.mutated.source.isEqual(q"""val rdd2 = rdd1.flatMap((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        originalValue.headOption
      })"""))

    assert(mutantHead.mutated.params.size == 1)
    assert(mutantHead.original.params.size == 1)
    assert(!mutantHead.mutated.params(0).isEqual(mutantHead.original.params(0)))
    assert(mutantHead.original.params(0).isEqual(q"""(x: String) => x.split(" ")"""))
    assert(mutantHead.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        originalValue.headOption
      }"""))

    // Tail Mutant
    assert(mutantTail.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantTail.original == original)
    assert(mutantTail.mutated != original)

    assert(mutantTail.mutated.id == mutantTail.original.id)
    assert(mutantTail.mutated.edges == mutantTail.original.edges)

    assert(mutantTail.mutated.name != mutantTail.original.name)
    assert(mutantTail.mutated.name == "flatMapTooriginalValue.tail")

    assert(mutantTail.mutated.source != mutantTail.original.source)
    assert(mutantTail.original.source.isEqual(q"""val rdd2 = rdd1.flatMap((x: String) => x.split(" "))"""))
    assert(mutantTail.mutated.source.isEqual(q"""val rdd2 = rdd1.flatMap((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        originalValue.tail
      })"""))

    assert(mutantTail.mutated.params.size == 1)
    assert(mutantTail.original.params.size == 1)
    assert(!mutantTail.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantTail.original.params(0).isEqual(q"""(x: String) => x.split(" ")"""))
    assert(mutantTail.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        originalValue.tail
      }"""))

    // Reverse Mutant
    assert(mutantReverse.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantReverse.original == original)
    assert(mutantReverse.mutated != original)

    assert(mutantReverse.mutated.id == mutantReverse.original.id)
    assert(mutantReverse.mutated.edges == mutantReverse.original.edges)

    assert(mutantReverse.mutated.name != mutantReverse.original.name)
    assert(mutantReverse.mutated.name == "flatMapTooriginalValue.reverse")

    assert(mutantReverse.mutated.source != mutantReverse.original.source)
    assert(mutantReverse.original.source.isEqual(q"""val rdd2 = rdd1.flatMap((x: String) => x.split(" "))"""))
    assert(mutantReverse.mutated.source.isEqual(q"""val rdd2 = rdd1.flatMap((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        originalValue.reverse
      })"""))

    assert(mutantReverse.mutated.params.size == 1)
    assert(mutantReverse.original.params.size == 1)
    assert(!mutantReverse.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantReverse.original.params(0).isEqual(q"""(x: String) => x.split(" ")"""))
    assert(mutantReverse.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        originalValue.reverse
      }"""))

    // Reverse Mutant
    assert(mutantEmpty.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantEmpty.original == original)
    assert(mutantEmpty.mutated != original)

    assert(mutantEmpty.mutated.id == mutantEmpty.original.id)
    assert(mutantEmpty.mutated.edges == mutantEmpty.original.edges)

    assert(mutantEmpty.mutated.name != mutantEmpty.original.name)
    assert(mutantEmpty.mutated.name == "flatMapToList[String]()")

    assert(mutantEmpty.mutated.source != mutantEmpty.original.source)
    assert(mutantEmpty.original.source.isEqual(q"""val rdd2 = rdd1.flatMap((x: String) => x.split(" "))"""))
    assert(mutantEmpty.mutated.source.isEqual(q"""val rdd2 = rdd1.flatMap((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        List[String]()
      })"""))

    assert(mutantEmpty.mutated.params.size == 1)
    assert(mutantEmpty.original.params.size == 1)
    assert(!mutantEmpty.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantEmpty.original.params(0).isEqual(q"""(x: String) => x.split(" ")"""))
    assert(mutantEmpty.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        List[String]()
      }"""))
  }

  test("Test Case 4 - Map to Int") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toInt)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 5)

    val mutant0 = mutants(0)
    val mutant1 = mutants(1)
    val mutantMax = mutants(2)
    val mutantMin = mutants(3)
    val mutantNegative = mutants(4)

    // Mutant mapTo0
    assert(mutant0.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant0.original == original)
    assert(mutant0.mutated != original)

    assert(mutant0.mutated.id == mutant0.original.id)
    assert(mutant0.mutated.edges == mutant0.original.edges)

    assert(mutant0.mutated.name != mutant0.original.name)
    assert(mutant0.mutated.name == "mapTo0")

    assert(mutant0.mutated.source != mutant0.original.source)
    assert(mutant0.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toInt)"""))
    assert(mutant0.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toInt
        val originalValue = originalFunction(inputParameter)
        0
      })"""))

    assert(mutant0.mutated.params.size == 1)
    assert(mutant0.original.params.size == 1)
    assert(!mutant0.mutated.params(0).isEqual(mutant0.original.params(0)))
    assert(mutant0.original.params(0).isEqual(q"""(x: String) => x.toInt"""))
    assert(mutant0.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toInt
        val originalValue = originalFunction(inputParameter)
        0
      }"""))

    // Mutant mapTo1
    assert(mutant1.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant1.original == original)
    assert(mutant1.mutated != original)

    assert(mutant1.mutated.id == mutant1.original.id)
    assert(mutant1.mutated.edges == mutant1.original.edges)

    assert(mutant1.mutated.name != mutant1.original.name)
    assert(mutant1.mutated.name == "mapTo1")

    assert(mutant1.mutated.source != mutant1.original.source)
    assert(mutant1.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toInt)"""))
    assert(mutant1.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toInt
        val originalValue = originalFunction(inputParameter)
        1
      })"""))

    assert(mutant1.mutated.params.size == 1)
    assert(mutant1.original.params.size == 1)
    assert(!mutant1.mutated.params(0).isEqual(mutant1.original.params(0)))
    assert(mutant1.original.params(0).isEqual(q"""(x: String) => x.toInt"""))
    assert(mutant1.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toInt
        val originalValue = originalFunction(inputParameter)
        1
      }"""))

    // Mutant mapToInt.MaxValue
    assert(mutantMax.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMax.original == original)
    assert(mutantMax.mutated != original)

    assert(mutantMax.mutated.id == mutantMax.original.id)
    assert(mutantMax.mutated.edges == mutantMax.original.edges)

    assert(mutantMax.mutated.name != mutantMax.original.name)
    assert(mutantMax.mutated.name == "mapToInt.MaxValue")

    assert(mutantMax.mutated.source != mutantMax.original.source)
    assert(mutantMax.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toInt)"""))
    assert(mutantMax.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toInt
        val originalValue = originalFunction(inputParameter)
        Int.MaxValue
      })"""))

    assert(mutantMax.mutated.params.size == 1)
    assert(mutantMax.original.params.size == 1)
    assert(!mutantMax.mutated.params(0).isEqual(mutantMax.original.params(0)))
    assert(mutantMax.original.params(0).isEqual(q"""(x: String) => x.toInt"""))
    assert(mutantMax.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toInt
        val originalValue = originalFunction(inputParameter)
        Int.MaxValue
      }"""))

    // Mutant mapToInt.MinValue
    assert(mutantMin.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMin.original == original)
    assert(mutantMin.mutated != original)

    assert(mutantMin.mutated.id == mutantMin.original.id)
    assert(mutantMin.mutated.edges == mutantMin.original.edges)

    assert(mutantMin.mutated.name != mutantMin.original.name)
    assert(mutantMin.mutated.name == "mapToInt.MinValue")

    assert(mutantMin.mutated.source != mutantMin.original.source)
    assert(mutantMin.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toInt)"""))
    assert(mutantMin.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toInt
        val originalValue = originalFunction(inputParameter)
        Int.MinValue
      })"""))

    assert(mutantMin.mutated.params.size == 1)
    assert(mutantMin.original.params.size == 1)
    assert(!mutantMin.mutated.params(0).isEqual(mutantMin.original.params(0)))
    assert(mutantMin.original.params(0).isEqual(q"""(x: String) => x.toInt"""))
    assert(mutantMin.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toInt
        val originalValue = originalFunction(inputParameter)
        Int.MinValue
      }"""))

    // Mutant mapToNegative
    assert(mutantNegative.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantNegative.original == original)
    assert(mutantNegative.mutated != original)

    assert(mutantNegative.mutated.id == mutantNegative.original.id)
    assert(mutantNegative.mutated.edges == mutantNegative.original.edges)

    assert(mutantNegative.mutated.name != mutantNegative.original.name)
    assert(mutantNegative.mutated.name == "mapTo-originalValue")

    assert(mutantNegative.mutated.source != mutantNegative.original.source)
    assert(mutantNegative.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toInt)"""))
    assert(mutantNegative.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toInt
        val originalValue = originalFunction(inputParameter)
        -originalValue
      })"""))

    assert(mutantNegative.mutated.params.size == 1)
    assert(mutantNegative.original.params.size == 1)
    assert(!mutantNegative.mutated.params(0).isEqual(mutantNegative.original.params(0)))
    assert(mutantNegative.original.params(0).isEqual(q"""(x: String) => x.toInt"""))
    assert(mutantNegative.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toInt
        val originalValue = originalFunction(inputParameter)
        -originalValue
      }"""))
  }

  test("Test Case 5 - Map to Long") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toLong)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Long)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 5)

    val mutant0 = mutants(0)
    val mutant1 = mutants(1)
    val mutantMax = mutants(2)
    val mutantMin = mutants(3)
    val mutantNegative = mutants(4)

    // Mutant mapTo0
    assert(mutant0.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant0.original == original)
    assert(mutant0.mutated != original)

    assert(mutant0.mutated.id == mutant0.original.id)
    assert(mutant0.mutated.edges == mutant0.original.edges)

    assert(mutant0.mutated.name != mutant0.original.name)
    assert(mutant0.mutated.name == "mapTo0l")

    assert(mutant0.mutated.source != mutant0.original.source)
    assert(mutant0.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toLong)"""))
    assert(mutant0.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toLong
        val originalValue = originalFunction(inputParameter)
        0l
      })"""))

    assert(mutant0.mutated.params.size == 1)
    assert(mutant0.original.params.size == 1)
    assert(!mutant0.mutated.params(0).isEqual(mutant0.original.params(0)))
    assert(mutant0.original.params(0).isEqual(q"""(x: String) => x.toLong"""))
    assert(mutant0.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toLong
        val originalValue = originalFunction(inputParameter)
        0l
      }"""))

    // Mutant mapTo1
    assert(mutant1.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant1.original == original)
    assert(mutant1.mutated != original)

    assert(mutant1.mutated.id == mutant1.original.id)
    assert(mutant1.mutated.edges == mutant1.original.edges)

    assert(mutant1.mutated.name != mutant1.original.name)
    assert(mutant1.mutated.name == "mapTo1l")

    assert(mutant1.mutated.source != mutant1.original.source)
    assert(mutant1.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toLong)"""))
    assert(mutant1.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toLong
        val originalValue = originalFunction(inputParameter)
        1l
      })"""))

    assert(mutant1.mutated.params.size == 1)
    assert(mutant1.original.params.size == 1)
    assert(!mutant1.mutated.params(0).isEqual(mutant1.original.params(0)))
    assert(mutant1.original.params(0).isEqual(q"""(x: String) => x.toLong"""))
    assert(mutant1.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toLong
        val originalValue = originalFunction(inputParameter)
        1l
      }"""))

    // Mutant mapToLong.MaxValue
    assert(mutantMax.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMax.original == original)
    assert(mutantMax.mutated != original)

    assert(mutantMax.mutated.id == mutantMax.original.id)
    assert(mutantMax.mutated.edges == mutantMax.original.edges)

    assert(mutantMax.mutated.name != mutantMax.original.name)
    assert(mutantMax.mutated.name == "mapToLong.MaxValue")

    assert(mutantMax.mutated.source != mutantMax.original.source)
    assert(mutantMax.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toLong)"""))
    assert(mutantMax.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toLong
        val originalValue = originalFunction(inputParameter)
        Long.MaxValue
      })"""))

    assert(mutantMax.mutated.params.size == 1)
    assert(mutantMax.original.params.size == 1)
    assert(!mutantMax.mutated.params(0).isEqual(mutantMax.original.params(0)))
    assert(mutantMax.original.params(0).isEqual(q"""(x: String) => x.toLong"""))
    assert(mutantMax.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toLong
        val originalValue = originalFunction(inputParameter)
        Long.MaxValue
      }"""))

    // Mutant mapToLong.MinValue
    assert(mutantMin.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMin.original == original)
    assert(mutantMin.mutated != original)

    assert(mutantMin.mutated.id == mutantMin.original.id)
    assert(mutantMin.mutated.edges == mutantMin.original.edges)

    assert(mutantMin.mutated.name != mutantMin.original.name)
    assert(mutantMin.mutated.name == "mapToLong.MinValue")

    assert(mutantMin.mutated.source != mutantMin.original.source)
    assert(mutantMin.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toLong)"""))
    assert(mutantMin.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toLong
        val originalValue = originalFunction(inputParameter)
        Long.MinValue
      })"""))

    assert(mutantMin.mutated.params.size == 1)
    assert(mutantMin.original.params.size == 1)
    assert(!mutantMin.mutated.params(0).isEqual(mutantMin.original.params(0)))
    assert(mutantMin.original.params(0).isEqual(q"""(x: String) => x.toLong"""))
    assert(mutantMin.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toLong
        val originalValue = originalFunction(inputParameter)
        Long.MinValue
      }"""))

    // Mutant mapToNegative
    assert(mutantNegative.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantNegative.original == original)
    assert(mutantNegative.mutated != original)

    assert(mutantNegative.mutated.id == mutantNegative.original.id)
    assert(mutantNegative.mutated.edges == mutantNegative.original.edges)

    assert(mutantNegative.mutated.name != mutantNegative.original.name)
    assert(mutantNegative.mutated.name == "mapTo-originalValue")

    assert(mutantNegative.mutated.source != mutantNegative.original.source)
    assert(mutantNegative.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toLong)"""))
    assert(mutantNegative.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toLong
        val originalValue = originalFunction(inputParameter)
        -originalValue
      })"""))

    assert(mutantNegative.mutated.params.size == 1)
    assert(mutantNegative.original.params.size == 1)
    assert(!mutantNegative.mutated.params(0).isEqual(mutantNegative.original.params(0)))
    assert(mutantNegative.original.params(0).isEqual(q"""(x: String) => x.toLong"""))
    assert(mutantNegative.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toLong
        val originalValue = originalFunction(inputParameter)
        -originalValue
      }"""))
  }

  test("Test Case 6 - Map to Float") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toFloat)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Float)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 5)

    val mutant0 = mutants(0)
    val mutant1 = mutants(1)
    val mutantMax = mutants(2)
    val mutantMin = mutants(3)
    val mutantNegative = mutants(4)

    // Mutant mapTo0
    assert(mutant0.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant0.original == original)
    assert(mutant0.mutated != original)

    assert(mutant0.mutated.id == mutant0.original.id)
    assert(mutant0.mutated.edges == mutant0.original.edges)

    assert(mutant0.mutated.name != mutant0.original.name)
    assert(mutant0.mutated.name == "mapTo0f")

    assert(mutant0.mutated.source != mutant0.original.source)
    assert(mutant0.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toFloat)"""))
    assert(mutant0.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toFloat
        val originalValue = originalFunction(inputParameter)
        0f
      })"""))

    assert(mutant0.mutated.params.size == 1)
    assert(mutant0.original.params.size == 1)
    assert(!mutant0.mutated.params(0).isEqual(mutant0.original.params(0)))
    assert(mutant0.original.params(0).isEqual(q"""(x: String) => x.toFloat"""))
    assert(mutant0.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toFloat
        val originalValue = originalFunction(inputParameter)
        0f
      }"""))

    // Mutant mapTo1
    assert(mutant1.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant1.original == original)
    assert(mutant1.mutated != original)

    assert(mutant1.mutated.id == mutant1.original.id)
    assert(mutant1.mutated.edges == mutant1.original.edges)

    assert(mutant1.mutated.name != mutant1.original.name)
    assert(mutant1.mutated.name == "mapTo1f")

    assert(mutant1.mutated.source != mutant1.original.source)
    assert(mutant1.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toFloat)"""))
    assert(mutant1.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toFloat
        val originalValue = originalFunction(inputParameter)
        1f
      })"""))

    assert(mutant1.mutated.params.size == 1)
    assert(mutant1.original.params.size == 1)
    assert(!mutant1.mutated.params(0).isEqual(mutant1.original.params(0)))
    assert(mutant1.original.params(0).isEqual(q"""(x: String) => x.toFloat"""))
    assert(mutant1.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toFloat
        val originalValue = originalFunction(inputParameter)
        1f
      }"""))

    // Mutant mapToFloat.MaxValue
    assert(mutantMax.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMax.original == original)
    assert(mutantMax.mutated != original)

    assert(mutantMax.mutated.id == mutantMax.original.id)
    assert(mutantMax.mutated.edges == mutantMax.original.edges)

    assert(mutantMax.mutated.name != mutantMax.original.name)
    assert(mutantMax.mutated.name == "mapToFloat.MaxValue")

    assert(mutantMax.mutated.source != mutantMax.original.source)
    assert(mutantMax.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toFloat)"""))
    assert(mutantMax.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toFloat
        val originalValue = originalFunction(inputParameter)
        Float.MaxValue
      })"""))

    assert(mutantMax.mutated.params.size == 1)
    assert(mutantMax.original.params.size == 1)
    assert(!mutantMax.mutated.params(0).isEqual(mutantMax.original.params(0)))
    assert(mutantMax.original.params(0).isEqual(q"""(x: String) => x.toFloat"""))
    assert(mutantMax.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toFloat
        val originalValue = originalFunction(inputParameter)
        Float.MaxValue
      }"""))

    // Mutant mapToFloat.MinValue
    assert(mutantMin.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMin.original == original)
    assert(mutantMin.mutated != original)

    assert(mutantMin.mutated.id == mutantMin.original.id)
    assert(mutantMin.mutated.edges == mutantMin.original.edges)

    assert(mutantMin.mutated.name != mutantMin.original.name)
    assert(mutantMin.mutated.name == "mapToFloat.MinValue")

    assert(mutantMin.mutated.source != mutantMin.original.source)
    assert(mutantMin.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toFloat)"""))
    assert(mutantMin.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toFloat
        val originalValue = originalFunction(inputParameter)
        Float.MinValue
      })"""))

    assert(mutantMin.mutated.params.size == 1)
    assert(mutantMin.original.params.size == 1)
    assert(!mutantMin.mutated.params(0).isEqual(mutantMin.original.params(0)))
    assert(mutantMin.original.params(0).isEqual(q"""(x: String) => x.toFloat"""))
    assert(mutantMin.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toFloat
        val originalValue = originalFunction(inputParameter)
        Float.MinValue
      }"""))

    // Mutant mapToNegative
    assert(mutantNegative.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantNegative.original == original)
    assert(mutantNegative.mutated != original)

    assert(mutantNegative.mutated.id == mutantNegative.original.id)
    assert(mutantNegative.mutated.edges == mutantNegative.original.edges)

    assert(mutantNegative.mutated.name != mutantNegative.original.name)
    assert(mutantNegative.mutated.name == "mapTo-originalValue")

    assert(mutantNegative.mutated.source != mutantNegative.original.source)
    assert(mutantNegative.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toFloat)"""))
    assert(mutantNegative.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toFloat
        val originalValue = originalFunction(inputParameter)
        -originalValue
      })"""))

    assert(mutantNegative.mutated.params.size == 1)
    assert(mutantNegative.original.params.size == 1)
    assert(!mutantNegative.mutated.params(0).isEqual(mutantNegative.original.params(0)))
    assert(mutantNegative.original.params(0).isEqual(q"""(x: String) => x.toFloat"""))
    assert(mutantNegative.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toFloat
        val originalValue = originalFunction(inputParameter)
        -originalValue
      }"""))
  }

  test("Test Case 7 - Map to Double") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toDouble)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Double)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 5)

    val mutant0 = mutants(0)
    val mutant1 = mutants(1)
    val mutantMax = mutants(2)
    val mutantMin = mutants(3)
    val mutantNegative = mutants(4)

    // Mutant mapTo0
    assert(mutant0.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant0.original == original)
    assert(mutant0.mutated != original)

    assert(mutant0.mutated.id == mutant0.original.id)
    assert(mutant0.mutated.edges == mutant0.original.edges)

    assert(mutant0.mutated.name != mutant0.original.name)
    assert(mutant0.mutated.name == "mapTo0d")

    assert(mutant0.mutated.source != mutant0.original.source)
    assert(mutant0.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toDouble)"""))
    assert(mutant0.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toDouble
        val originalValue = originalFunction(inputParameter)
        0d
      })"""))

    assert(mutant0.mutated.params.size == 1)
    assert(mutant0.original.params.size == 1)
    assert(!mutant0.mutated.params(0).isEqual(mutant0.original.params(0)))
    assert(mutant0.original.params(0).isEqual(q"""(x: String) => x.toDouble"""))
    assert(mutant0.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toDouble
        val originalValue = originalFunction(inputParameter)
        0d
      }"""))

    // Mutant mapTo1
    assert(mutant1.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant1.original == original)
    assert(mutant1.mutated != original)

    assert(mutant1.mutated.id == mutant1.original.id)
    assert(mutant1.mutated.edges == mutant1.original.edges)

    assert(mutant1.mutated.name != mutant1.original.name)
    assert(mutant1.mutated.name == "mapTo1d")

    assert(mutant1.mutated.source != mutant1.original.source)
    assert(mutant1.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toDouble)"""))
    assert(mutant1.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toDouble
        val originalValue = originalFunction(inputParameter)
        1d
      })"""))

    assert(mutant1.mutated.params.size == 1)
    assert(mutant1.original.params.size == 1)
    assert(!mutant1.mutated.params(0).isEqual(mutant1.original.params(0)))
    assert(mutant1.original.params(0).isEqual(q"""(x: String) => x.toDouble"""))
    assert(mutant1.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toDouble
        val originalValue = originalFunction(inputParameter)
        1d
      }"""))

    // Mutant mapToDouble.MaxValue
    assert(mutantMax.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMax.original == original)
    assert(mutantMax.mutated != original)

    assert(mutantMax.mutated.id == mutantMax.original.id)
    assert(mutantMax.mutated.edges == mutantMax.original.edges)

    assert(mutantMax.mutated.name != mutantMax.original.name)
    assert(mutantMax.mutated.name == "mapToDouble.MaxValue")

    assert(mutantMax.mutated.source != mutantMax.original.source)
    assert(mutantMax.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toDouble)"""))
    assert(mutantMax.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toDouble
        val originalValue = originalFunction(inputParameter)
        Double.MaxValue
      })"""))

    assert(mutantMax.mutated.params.size == 1)
    assert(mutantMax.original.params.size == 1)
    assert(!mutantMax.mutated.params(0).isEqual(mutantMax.original.params(0)))
    assert(mutantMax.original.params(0).isEqual(q"""(x: String) => x.toDouble"""))
    assert(mutantMax.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toDouble
        val originalValue = originalFunction(inputParameter)
        Double.MaxValue
      }"""))

    // Mutant mapToFloat.MinValue
    assert(mutantMin.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMin.original == original)
    assert(mutantMin.mutated != original)

    assert(mutantMin.mutated.id == mutantMin.original.id)
    assert(mutantMin.mutated.edges == mutantMin.original.edges)

    assert(mutantMin.mutated.name != mutantMin.original.name)
    assert(mutantMin.mutated.name == "mapToDouble.MinValue")

    assert(mutantMin.mutated.source != mutantMin.original.source)
    assert(mutantMin.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toDouble)"""))
    assert(mutantMin.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toDouble
        val originalValue = originalFunction(inputParameter)
        Double.MinValue
      })"""))

    assert(mutantMin.mutated.params.size == 1)
    assert(mutantMin.original.params.size == 1)
    assert(!mutantMin.mutated.params(0).isEqual(mutantMin.original.params(0)))
    assert(mutantMin.original.params(0).isEqual(q"""(x: String) => x.toDouble"""))
    assert(mutantMin.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toDouble
        val originalValue = originalFunction(inputParameter)
        Double.MinValue
      }"""))

    // Mutant mapToNegative
    assert(mutantNegative.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantNegative.original == original)
    assert(mutantNegative.mutated != original)

    assert(mutantNegative.mutated.id == mutantNegative.original.id)
    assert(mutantNegative.mutated.edges == mutantNegative.original.edges)

    assert(mutantNegative.mutated.name != mutantNegative.original.name)
    assert(mutantNegative.mutated.name == "mapTo-originalValue")

    assert(mutantNegative.mutated.source != mutantNegative.original.source)
    assert(mutantNegative.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toDouble)"""))
    assert(mutantNegative.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toDouble
        val originalValue = originalFunction(inputParameter)
        -originalValue
      })"""))

    assert(mutantNegative.mutated.params.size == 1)
    assert(mutantNegative.original.params.size == 1)
    assert(!mutantNegative.mutated.params(0).isEqual(mutantNegative.original.params(0)))
    assert(mutantNegative.original.params(0).isEqual(q"""(x: String) => x.toDouble"""))
    assert(mutantNegative.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toDouble
        val originalValue = originalFunction(inputParameter)
        -originalValue
      }"""))
  }

  test("Test Case 8 - Map to Char") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toChar)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Char)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 5)

    val mutant0 = mutants(0)
    val mutant1 = mutants(1)
    val mutantMax = mutants(2)
    val mutantMin = mutants(3)
    val mutantNegative = mutants(4)

    // Mutant mapTo0
    assert(mutant0.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant0.original == original)
    assert(mutant0.mutated != original)

    assert(mutant0.mutated.id == mutant0.original.id)
    assert(mutant0.mutated.edges == mutant0.original.edges)

    assert(mutant0.mutated.name != mutant0.original.name)
    assert(mutant0.mutated.name == "mapTo0.toChar")

    assert(mutant0.mutated.source != mutant0.original.source)
    assert(mutant0.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toChar)"""))
    assert(mutant0.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toChar
        val originalValue = originalFunction(inputParameter)
        0.toChar
      })"""))

    assert(mutant0.mutated.params.size == 1)
    assert(mutant0.original.params.size == 1)
    assert(!mutant0.mutated.params(0).isEqual(mutant0.original.params(0)))
    assert(mutant0.original.params(0).isEqual(q"""(x: String) => x.toChar"""))
    assert(mutant0.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toChar
        val originalValue = originalFunction(inputParameter)
        0.toChar
      }"""))

    // Mutant mapTo1
    assert(mutant1.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant1.original == original)
    assert(mutant1.mutated != original)

    assert(mutant1.mutated.id == mutant1.original.id)
    assert(mutant1.mutated.edges == mutant1.original.edges)

    assert(mutant1.mutated.name != mutant1.original.name)
    assert(mutant1.mutated.name == "mapTo1.toChar")

    assert(mutant1.mutated.source != mutant1.original.source)
    assert(mutant1.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toChar)"""))
    assert(mutant1.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toChar
        val originalValue = originalFunction(inputParameter)
        1.toChar
      })"""))

    assert(mutant1.mutated.params.size == 1)
    assert(mutant1.original.params.size == 1)
    assert(!mutant1.mutated.params(0).isEqual(mutant1.original.params(0)))
    assert(mutant1.original.params(0).isEqual(q"""(x: String) => x.toChar"""))
    assert(mutant1.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toChar
        val originalValue = originalFunction(inputParameter)
        1.toChar
      }"""))

    // Mutant mapToChar.MaxValue
    assert(mutantMax.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMax.original == original)
    assert(mutantMax.mutated != original)

    assert(mutantMax.mutated.id == mutantMax.original.id)
    assert(mutantMax.mutated.edges == mutantMax.original.edges)

    assert(mutantMax.mutated.name != mutantMax.original.name)
    assert(mutantMax.mutated.name == "mapToChar.MaxValue")

    assert(mutantMax.mutated.source != mutantMax.original.source)
    assert(mutantMax.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toChar)"""))
    assert(mutantMax.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toChar
        val originalValue = originalFunction(inputParameter)
        Char.MaxValue
      })"""))

    assert(mutantMax.mutated.params.size == 1)
    assert(mutantMax.original.params.size == 1)
    assert(!mutantMax.mutated.params(0).isEqual(mutantMax.original.params(0)))
    assert(mutantMax.original.params(0).isEqual(q"""(x: String) => x.toChar"""))
    assert(mutantMax.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toChar
        val originalValue = originalFunction(inputParameter)
        Char.MaxValue
      }"""))

    // Mutant mapToInt.MinValue
    assert(mutantMin.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMin.original == original)
    assert(mutantMin.mutated != original)

    assert(mutantMin.mutated.id == mutantMin.original.id)
    assert(mutantMin.mutated.edges == mutantMin.original.edges)

    assert(mutantMin.mutated.name != mutantMin.original.name)
    assert(mutantMin.mutated.name == "mapToChar.MinValue")

    assert(mutantMin.mutated.source != mutantMin.original.source)
    assert(mutantMin.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toChar)"""))
    assert(mutantMin.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toChar
        val originalValue = originalFunction(inputParameter)
        Char.MinValue
      })"""))

    assert(mutantMin.mutated.params.size == 1)
    assert(mutantMin.original.params.size == 1)
    assert(!mutantMin.mutated.params(0).isEqual(mutantMin.original.params(0)))
    assert(mutantMin.original.params(0).isEqual(q"""(x: String) => x.toChar"""))
    assert(mutantMin.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toChar
        val originalValue = originalFunction(inputParameter)
        Char.MinValue
      }"""))

    // Mutant mapToNegative
    assert(mutantNegative.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantNegative.original == original)
    assert(mutantNegative.mutated != original)

    assert(mutantNegative.mutated.id == mutantNegative.original.id)
    assert(mutantNegative.mutated.edges == mutantNegative.original.edges)

    assert(mutantNegative.mutated.name != mutantNegative.original.name)
    assert(mutantNegative.mutated.name == "mapTo-originalValue")

    assert(mutantNegative.mutated.source != mutantNegative.original.source)
    assert(mutantNegative.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toChar)"""))
    assert(mutantNegative.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toChar
        val originalValue = originalFunction(inputParameter)
        -originalValue
      })"""))

    assert(mutantNegative.mutated.params.size == 1)
    assert(mutantNegative.original.params.size == 1)
    assert(!mutantNegative.mutated.params(0).isEqual(mutantNegative.original.params(0)))
    assert(mutantNegative.original.params(0).isEqual(q"""(x: String) => x.toChar"""))
    assert(mutantNegative.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toChar
        val originalValue = originalFunction(inputParameter)
        -originalValue
      }"""))
  }

  test("Test Case 9 - Map to Boolean") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => !x.isEmpty)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Boolean)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 3)

    val mutantFalse = mutants(0)
    val mutantTrue = mutants(1)
    val mutantNegation = mutants(2)

    // Mutant mapTofalse
    assert(mutantFalse.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantFalse.original == original)
    assert(mutantFalse.mutated != original)

    assert(mutantFalse.mutated.id == mutantFalse.original.id)
    assert(mutantFalse.mutated.edges == mutantFalse.original.edges)

    assert(mutantFalse.mutated.name != mutantFalse.original.name)
    assert(mutantFalse.mutated.name == "mapTofalse")

    assert(mutantFalse.mutated.source != mutantFalse.original.source)
    assert(mutantFalse.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => !x.isEmpty)"""))
    assert(mutantFalse.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => !x.isEmpty
        val originalValue = originalFunction(inputParameter)
        false
      })"""))

    assert(mutantFalse.mutated.params.size == 1)
    assert(mutantFalse.original.params.size == 1)
    assert(!mutantFalse.mutated.params(0).isEqual(mutantFalse.original.params(0)))
    assert(mutantFalse.original.params(0).isEqual(q"""(x: String) => !x.isEmpty"""))
    assert(mutantFalse.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => !x.isEmpty
        val originalValue = originalFunction(inputParameter)
        false
      }"""))

    // Mutant mapTotrue
    assert(mutantTrue.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantTrue.original == original)
    assert(mutantTrue.mutated != original)

    assert(mutantTrue.mutated.id == mutantTrue.original.id)
    assert(mutantTrue.mutated.edges == mutantTrue.original.edges)

    assert(mutantTrue.mutated.name != mutantTrue.original.name)
    assert(mutantTrue.mutated.name == "mapTotrue")

    assert(mutantTrue.mutated.source != mutantTrue.original.source)
    assert(mutantTrue.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => !x.isEmpty)"""))
    assert(mutantTrue.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => !x.isEmpty
        val originalValue = originalFunction(inputParameter)
        true
      })"""))

    assert(mutantTrue.mutated.params.size == 1)
    assert(mutantTrue.original.params.size == 1)
    assert(!mutantTrue.mutated.params(0).isEqual(mutantFalse.original.params(0)))
    assert(mutantTrue.original.params(0).isEqual(q"""(x: String) => !x.isEmpty"""))
    assert(mutantTrue.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => !x.isEmpty
        val originalValue = originalFunction(inputParameter)
        true
      }"""))

    // Mutant mapTo!originalValue
    assert(mutantNegation.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantNegation.original == original)
    assert(mutantNegation.mutated != original)

    assert(mutantNegation.mutated.id == mutantNegation.original.id)
    assert(mutantNegation.mutated.edges == mutantNegation.original.edges)

    assert(mutantNegation.mutated.name != mutantNegation.original.name)
    assert(mutantNegation.mutated.name == "mapTo!originalValue")

    assert(mutantNegation.mutated.source != mutantNegation.original.source)
    assert(mutantNegation.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => !x.isEmpty)"""))
    assert(mutantNegation.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => !x.isEmpty
        val originalValue = originalFunction(inputParameter)
        !originalValue
      })"""))

    assert(mutantNegation.mutated.params.size == 1)
    assert(mutantNegation.original.params.size == 1)
    assert(!mutantNegation.mutated.params(0).isEqual(mutantFalse.original.params(0)))
    assert(mutantNegation.original.params(0).isEqual(q"""(x: String) => !x.isEmpty"""))
    assert(mutantNegation.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => !x.isEmpty
        val originalValue = originalFunction(inputParameter)
        !originalValue
      }"""))
  }

  test("Test Case 10 - Map to String") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toUpperCase)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutantEmpty = mutants(0)

    // Mutant mapTo""
    assert(mutantEmpty.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantEmpty.original == original)
    assert(mutantEmpty.mutated != original)

    assert(mutantEmpty.mutated.id == mutantEmpty.original.id)
    assert(mutantEmpty.mutated.edges == mutantEmpty.original.edges)

    assert(mutantEmpty.mutated.name != mutantEmpty.original.name)
    assert(mutantEmpty.mutated.name == "mapTo\"\"")

    assert(mutantEmpty.mutated.source != mutantEmpty.original.source)
    assert(mutantEmpty.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.toUpperCase)"""))
    assert(mutantEmpty.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.toUpperCase
        val originalValue = originalFunction(inputParameter)
        ""
      })"""))

    assert(mutantEmpty.mutated.params.size == 1)
    assert(mutantEmpty.original.params.size == 1)
    assert(!mutantEmpty.mutated.params(0).isEqual(mutantEmpty.original.params(0)))
    assert(mutantEmpty.original.params(0).isEqual(q"""(x: String) => x.toUpperCase"""))
    assert(mutantEmpty.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.toUpperCase
        val originalValue = originalFunction(inputParameter)
        ""
      }"""))
  }

  test("Test Case 11 - Map to Option[String]") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => if(!x.isEmpty) Some(x) else None)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(ParameterizedType("Option", List(BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutantEmpty = mutants(0)

    // Mutant mapTo""
    assert(mutantEmpty.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantEmpty.original == original)
    assert(mutantEmpty.mutated != original)

    assert(mutantEmpty.mutated.id == mutantEmpty.original.id)
    assert(mutantEmpty.mutated.edges == mutantEmpty.original.edges)

    assert(mutantEmpty.mutated.name != mutantEmpty.original.name)
    assert(mutantEmpty.mutated.name == "mapToList[String]().headOption")

    assert(mutantEmpty.mutated.source != mutantEmpty.original.source)
    assert(mutantEmpty.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => if(!x.isEmpty) Some(x) else None)"""))
    assert(mutantEmpty.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => if(!x.isEmpty) Some(x) else None
        val originalValue = originalFunction(inputParameter)
        List[String]().headOption
      })"""))

    assert(mutantEmpty.mutated.params.size == 1)
    assert(mutantEmpty.original.params.size == 1)
    assert(!mutantEmpty.mutated.params(0).isEqual(mutantEmpty.original.params(0)))
    assert(mutantEmpty.original.params(0).isEqual(q"""(x: String) => if(!x.isEmpty) Some(x) else None"""))
    assert(mutantEmpty.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => if(!x.isEmpty) Some(x) else None
        val originalValue = originalFunction(inputParameter)
        List[String]().headOption
      }"""))
  }

  test("Test Case 12 - Map to List[String]") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.split(" "))
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(ParameterizedType("List", List(BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 4)

    val mutantHead = mutants(0)
    val mutantTail = mutants(1)
    val mutantReverse = mutants(2)
    val mutantEmpty = mutants(3)

    // Head Mutant
    assert(mutantHead.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantHead.original == original)
    assert(mutantHead.mutated != original)

    assert(mutantHead.mutated.id == mutantHead.original.id)
    assert(mutantHead.mutated.edges == mutantHead.original.edges)

    assert(mutantHead.mutated.name != mutantHead.original.name)
    assert(mutantHead.mutated.name == "mapToList[String](originalValue.head)")

    assert(mutantHead.mutated.source != mutantHead.original.source)
    assert(mutantHead.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" "))"""))
    assert(mutantHead.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        List[String](originalValue.head)
      })"""))

    assert(mutantHead.mutated.params.size == 1)
    assert(mutantHead.original.params.size == 1)
    assert(!mutantHead.mutated.params(0).isEqual(mutantHead.original.params(0)))
    assert(mutantHead.original.params(0).isEqual(q"""(x: String) => x.split(" ")"""))
    assert(mutantHead.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        List[String](originalValue.head)
      }"""))

    // Tail Mutant
    assert(mutantTail.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantTail.original == original)
    assert(mutantTail.mutated != original)

    assert(mutantTail.mutated.id == mutantTail.original.id)
    assert(mutantTail.mutated.edges == mutantTail.original.edges)

    assert(mutantTail.mutated.name != mutantTail.original.name)
    assert(mutantTail.mutated.name == "mapTooriginalValue.tail")

    assert(mutantTail.mutated.source != mutantTail.original.source)
    assert(mutantTail.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" "))"""))
    assert(mutantTail.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        originalValue.tail
      })"""))

    assert(mutantTail.mutated.params.size == 1)
    assert(mutantTail.original.params.size == 1)
    assert(!mutantTail.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantTail.original.params(0).isEqual(q"""(x: String) => x.split(" ")"""))
    assert(mutantTail.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        originalValue.tail
      }"""))

    // Reverse Mutant
    assert(mutantReverse.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantReverse.original == original)
    assert(mutantReverse.mutated != original)

    assert(mutantReverse.mutated.id == mutantReverse.original.id)
    assert(mutantReverse.mutated.edges == mutantReverse.original.edges)

    assert(mutantReverse.mutated.name != mutantReverse.original.name)
    assert(mutantReverse.mutated.name == "mapTooriginalValue.reverse")

    assert(mutantReverse.mutated.source != mutantReverse.original.source)
    assert(mutantReverse.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" "))"""))
    assert(mutantReverse.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        originalValue.reverse
      })"""))

    assert(mutantReverse.mutated.params.size == 1)
    assert(mutantReverse.original.params.size == 1)
    assert(!mutantReverse.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantReverse.original.params(0).isEqual(q"""(x: String) => x.split(" ")"""))
    assert(mutantReverse.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        originalValue.reverse
      }"""))

    // Reverse Mutant
    assert(mutantEmpty.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantEmpty.original == original)
    assert(mutantEmpty.mutated != original)

    assert(mutantEmpty.mutated.id == mutantEmpty.original.id)
    assert(mutantEmpty.mutated.edges == mutantEmpty.original.edges)

    assert(mutantEmpty.mutated.name != mutantEmpty.original.name)
    assert(mutantEmpty.mutated.name == "mapToList[String]()")

    assert(mutantEmpty.mutated.source != mutantEmpty.original.source)
    assert(mutantEmpty.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" "))"""))
    assert(mutantEmpty.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        List[String]()
      })"""))

    assert(mutantEmpty.mutated.params.size == 1)
    assert(mutantEmpty.original.params.size == 1)
    assert(!mutantEmpty.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantEmpty.original.params(0).isEqual(q"""(x: String) => x.split(" ")"""))
    assert(mutantEmpty.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ")
        val originalValue = originalFunction(inputParameter)
        List[String]()
      }"""))
  }

  test("Test Case 13 - Map to Array[String]") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.split(" ").toArray)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(ParameterizedType("Array", List(BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 4)

    val mutantHead = mutants(0)
    val mutantTail = mutants(1)
    val mutantReverse = mutants(2)
    val mutantEmpty = mutants(3)

    // Head Mutant
    assert(mutantHead.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantHead.original == original)
    assert(mutantHead.mutated != original)

    assert(mutantHead.mutated.id == mutantHead.original.id)
    assert(mutantHead.mutated.edges == mutantHead.original.edges)

    assert(mutantHead.mutated.name != mutantHead.original.name)
    assert(mutantHead.mutated.name == "mapToArray[String](originalValue.head)")

    assert(mutantHead.mutated.source != mutantHead.original.source)
    assert(mutantHead.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" ").toArray)"""))
    assert(mutantHead.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toArray
        val originalValue = originalFunction(inputParameter)
        Array[String](originalValue.head)
      })"""))

    assert(mutantHead.mutated.params.size == 1)
    assert(mutantHead.original.params.size == 1)
    assert(!mutantHead.mutated.params(0).isEqual(mutantHead.original.params(0)))
    assert(mutantHead.original.params(0).isEqual(q"""(x: String) => x.split(" ").toArray"""))
    assert(mutantHead.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toArray
        val originalValue = originalFunction(inputParameter)
        Array[String](originalValue.head)
      }"""))

    // Tail Mutant
    assert(mutantTail.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantTail.original == original)
    assert(mutantTail.mutated != original)

    assert(mutantTail.mutated.id == mutantTail.original.id)
    assert(mutantTail.mutated.edges == mutantTail.original.edges)

    assert(mutantTail.mutated.name != mutantTail.original.name)
    assert(mutantTail.mutated.name == "mapTooriginalValue.tail")

    assert(mutantTail.mutated.source != mutantTail.original.source)
    assert(mutantTail.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" ").toArray)"""))
    assert(mutantTail.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toArray
        val originalValue = originalFunction(inputParameter)
        originalValue.tail
      })"""))

    assert(mutantTail.mutated.params.size == 1)
    assert(mutantTail.original.params.size == 1)
    assert(!mutantTail.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantTail.original.params(0).isEqual(q"""(x: String) => x.split(" ").toArray"""))
    assert(mutantTail.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toArray
        val originalValue = originalFunction(inputParameter)
        originalValue.tail
      }"""))

    // Reverse Mutant
    assert(mutantReverse.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantReverse.original == original)
    assert(mutantReverse.mutated != original)

    assert(mutantReverse.mutated.id == mutantReverse.original.id)
    assert(mutantReverse.mutated.edges == mutantReverse.original.edges)

    assert(mutantReverse.mutated.name != mutantReverse.original.name)
    assert(mutantReverse.mutated.name == "mapTooriginalValue.reverse")

    assert(mutantReverse.mutated.source != mutantReverse.original.source)
    assert(mutantReverse.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" ").toArray)"""))
    assert(mutantReverse.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toArray
        val originalValue = originalFunction(inputParameter)
        originalValue.reverse
      })"""))

    assert(mutantReverse.mutated.params.size == 1)
    assert(mutantReverse.original.params.size == 1)
    assert(!mutantReverse.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantReverse.original.params(0).isEqual(q"""(x: String) => x.split(" ").toArray"""))
    assert(mutantReverse.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toArray
        val originalValue = originalFunction(inputParameter)
        originalValue.reverse
      }"""))

    // Reverse Mutant
    assert(mutantEmpty.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantEmpty.original == original)
    assert(mutantEmpty.mutated != original)

    assert(mutantEmpty.mutated.id == mutantEmpty.original.id)
    assert(mutantEmpty.mutated.edges == mutantEmpty.original.edges)

    assert(mutantEmpty.mutated.name != mutantEmpty.original.name)
    assert(mutantEmpty.mutated.name == "mapToArray[String]()")

    assert(mutantEmpty.mutated.source != mutantEmpty.original.source)
    assert(mutantEmpty.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" ").toArray)"""))
    assert(mutantEmpty.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toArray
        val originalValue = originalFunction(inputParameter)
        Array[String]()
      })"""))

    assert(mutantEmpty.mutated.params.size == 1)
    assert(mutantEmpty.original.params.size == 1)
    assert(!mutantEmpty.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantEmpty.original.params(0).isEqual(q"""(x: String) => x.split(" ").toArray"""))
    assert(mutantEmpty.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toArray
        val originalValue = originalFunction(inputParameter)
        Array[String]()
      }"""))
  }

  test("Test Case 14 - Map to Set[String]") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.split(" ").toSet)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(ParameterizedType("Set", List(BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 3)

    val mutantHead = mutants(0)
    val mutantTail = mutants(1)
    val mutantEmpty = mutants(2)

    // Head Mutant
    assert(mutantHead.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantHead.original == original)
    assert(mutantHead.mutated != original)

    assert(mutantHead.mutated.id == mutantHead.original.id)
    assert(mutantHead.mutated.edges == mutantHead.original.edges)

    assert(mutantHead.mutated.name != mutantHead.original.name)
    assert(mutantHead.mutated.name == "mapToSet[String](originalValue.head)")

    assert(mutantHead.mutated.source != mutantHead.original.source)
    assert(mutantHead.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" ").toSet)"""))
    assert(mutantHead.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toSet
        val originalValue = originalFunction(inputParameter)
        Set[String](originalValue.head)
      })"""))

    assert(mutantHead.mutated.params.size == 1)
    assert(mutantHead.original.params.size == 1)
    assert(!mutantHead.mutated.params(0).isEqual(mutantHead.original.params(0)))
    assert(mutantHead.original.params(0).isEqual(q"""(x: String) => x.split(" ").toSet"""))
    assert(mutantHead.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toSet
        val originalValue = originalFunction(inputParameter)
        Set[String](originalValue.head)
      }"""))

    // Tail Mutant
    assert(mutantTail.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantTail.original == original)
    assert(mutantTail.mutated != original)

    assert(mutantTail.mutated.id == mutantTail.original.id)
    assert(mutantTail.mutated.edges == mutantTail.original.edges)

    assert(mutantTail.mutated.name != mutantTail.original.name)
    assert(mutantTail.mutated.name == "mapTooriginalValue.tail")

    assert(mutantTail.mutated.source != mutantTail.original.source)
    assert(mutantTail.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" ").toSet)"""))
    assert(mutantTail.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toSet
        val originalValue = originalFunction(inputParameter)
        originalValue.tail
      })"""))

    assert(mutantTail.mutated.params.size == 1)
    assert(mutantTail.original.params.size == 1)
    assert(!mutantTail.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantTail.original.params(0).isEqual(q"""(x: String) => x.split(" ").toSet"""))
    assert(mutantTail.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toSet
        val originalValue = originalFunction(inputParameter)
        originalValue.tail
      }"""))

    // Reverse Mutant
    assert(mutantEmpty.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantEmpty.original == original)
    assert(mutantEmpty.mutated != original)

    assert(mutantEmpty.mutated.id == mutantEmpty.original.id)
    assert(mutantEmpty.mutated.edges == mutantEmpty.original.edges)

    assert(mutantEmpty.mutated.name != mutantEmpty.original.name)
    assert(mutantEmpty.mutated.name == "mapToSet[String]()")

    assert(mutantEmpty.mutated.source != mutantEmpty.original.source)
    assert(mutantEmpty.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => x.split(" ").toSet)"""))
    assert(mutantEmpty.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toSet
        val originalValue = originalFunction(inputParameter)
        Set[String]()
      })"""))

    assert(mutantEmpty.mutated.params.size == 1)
    assert(mutantEmpty.original.params.size == 1)
    assert(!mutantEmpty.mutated.params(0).isEqual(mutantTail.original.params(0)))
    assert(mutantEmpty.original.params(0).isEqual(q"""(x: String) => x.split(" ").toSet"""))
    assert(mutantEmpty.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => x.split(" ").toSet
        val originalValue = originalFunction(inputParameter)
        Set[String]()
      }"""))
  }

  test("Test Case 15 - Map to (Int, Boolean)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => (x.toInt, !x.isEmpty))
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Int), BaseType(BaseTypesEnum.Boolean))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 8)

    val mutant0 = mutants(0)
    val mutant1 = mutants(1)
    val mutantMax = mutants(2)
    val mutantMin = mutants(3)
    val mutantNegative = mutants(4)
    val mutantFalse = mutants(5)
    val mutantTrue = mutants(6)
    val mutantNegation = mutants(7)

    // Mutant mapTo0
    assert(mutant0.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant0.original == original)
    assert(mutant0.mutated != original)

    assert(mutant0.mutated.id == mutant0.original.id)
    assert(mutant0.mutated.edges == mutant0.original.edges)

    assert(mutant0.mutated.name != mutant0.original.name)
    assert(mutant0.mutated.name == "mapTo(0, originalValue._2)")

    assert(mutant0.mutated.source != mutant0.original.source)
    assert(mutant0.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => (x.toInt, !x.isEmpty))"""))
    assert(mutant0.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (0, originalValue._2)
      })"""))

    assert(mutant0.mutated.params.size == 1)
    assert(mutant0.original.params.size == 1)
    assert(!mutant0.mutated.params(0).isEqual(mutant0.original.params(0)))
    assert(mutant0.original.params(0).isEqual(q"""(x: String) => (x.toInt, !x.isEmpty)"""))
    assert(mutant0.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (0, originalValue._2)
      }"""))

    // Mutant mapTo1
    assert(mutant1.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutant1.original == original)
    assert(mutant1.mutated != original)

    assert(mutant1.mutated.id == mutant1.original.id)
    assert(mutant1.mutated.edges == mutant1.original.edges)

    assert(mutant1.mutated.name != mutant1.original.name)
    assert(mutant1.mutated.name == "mapTo(1, originalValue._2)")

    assert(mutant1.mutated.source != mutant1.original.source)
    assert(mutant1.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => (x.toInt, !x.isEmpty))"""))
    assert(mutant1.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (1, originalValue._2)
      })"""))

    assert(mutant1.mutated.params.size == 1)
    assert(mutant1.original.params.size == 1)
    assert(!mutant1.mutated.params(0).isEqual(mutant1.original.params(0)))
    assert(mutant1.original.params(0).isEqual(q"""(x: String) => (x.toInt, !x.isEmpty)"""))
    assert(mutant1.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (1, originalValue._2)
      }"""))

    // Mutant mapToInt.MaxValue
    assert(mutantMax.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMax.original == original)
    assert(mutantMax.mutated != original)

    assert(mutantMax.mutated.id == mutantMax.original.id)
    assert(mutantMax.mutated.edges == mutantMax.original.edges)

    assert(mutantMax.mutated.name != mutantMax.original.name)
    assert(mutantMax.mutated.name == "mapTo(Int.MaxValue, originalValue._2)")

    assert(mutantMax.mutated.source != mutantMax.original.source)
    assert(mutantMax.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => (x.toInt, !x.isEmpty))"""))
    assert(mutantMax.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (Int.MaxValue, originalValue._2)
      })"""))

    assert(mutantMax.mutated.params.size == 1)
    assert(mutantMax.original.params.size == 1)
    assert(!mutantMax.mutated.params(0).isEqual(mutantMax.original.params(0)))
    assert(mutantMax.original.params(0).isEqual(q"""(x: String) => (x.toInt, !x.isEmpty)"""))
    assert(mutantMax.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (Int.MaxValue, originalValue._2)
      }"""))

    // Mutant mapToInt.MinValue
    assert(mutantMin.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantMin.original == original)
    assert(mutantMin.mutated != original)

    assert(mutantMin.mutated.id == mutantMin.original.id)
    assert(mutantMin.mutated.edges == mutantMin.original.edges)

    assert(mutantMin.mutated.name != mutantMin.original.name)
    assert(mutantMin.mutated.name == "mapTo(Int.MinValue, originalValue._2)")

    assert(mutantMin.mutated.source != mutantMin.original.source)
    assert(mutantMin.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => (x.toInt, !x.isEmpty))"""))
    assert(mutantMin.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (Int.MinValue, originalValue._2)
      })"""))

    assert(mutantMin.mutated.params.size == 1)
    assert(mutantMin.original.params.size == 1)
    assert(!mutantMin.mutated.params(0).isEqual(mutantMin.original.params(0)))
    assert(mutantMin.original.params(0).isEqual(q"""(x: String) => (x.toInt, !x.isEmpty)"""))
    assert(mutantMin.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (Int.MinValue, originalValue._2)
      }"""))

    // Mutant mapToNegative
    assert(mutantNegative.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantNegative.original == original)
    assert(mutantNegative.mutated != original)

    assert(mutantNegative.mutated.id == mutantNegative.original.id)
    assert(mutantNegative.mutated.edges == mutantNegative.original.edges)

    assert(mutantNegative.mutated.name != mutantNegative.original.name)
    assert(mutantNegative.mutated.name == "mapTo(-originalValue._1, originalValue._2)")

    assert(mutantNegative.mutated.source != mutantNegative.original.source)
    assert(mutantNegative.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => (x.toInt, !x.isEmpty))"""))
    assert(mutantNegative.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (-originalValue._1, originalValue._2)
      })"""))

    assert(mutantNegative.mutated.params.size == 1)
    assert(mutantNegative.original.params.size == 1)
    assert(!mutantNegative.mutated.params(0).isEqual(mutantNegative.original.params(0)))
    assert(mutantNegative.original.params(0).isEqual(q"""(x: String) => (x.toInt, !x.isEmpty)"""))
    assert(mutantNegative.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (-originalValue._1, originalValue._2)
      }"""))

    // Mutant mapTofalse
    assert(mutantFalse.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantFalse.original == original)
    assert(mutantFalse.mutated != original)

    assert(mutantFalse.mutated.id == mutantFalse.original.id)
    assert(mutantFalse.mutated.edges == mutantFalse.original.edges)

    assert(mutantFalse.mutated.name != mutantFalse.original.name)
    assert(mutantFalse.mutated.name == "mapTo(originalValue._1, false)")

    assert(mutantFalse.mutated.source != mutantFalse.original.source)
    assert(mutantFalse.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => (x.toInt, !x.isEmpty))"""))
    assert(mutantFalse.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (originalValue._1, false)
      })"""))

    assert(mutantFalse.mutated.params.size == 1)
    assert(mutantFalse.original.params.size == 1)
    assert(!mutantFalse.mutated.params(0).isEqual(mutantFalse.original.params(0)))
    assert(mutantFalse.original.params(0).isEqual(q"""(x: String) => (x.toInt, !x.isEmpty)"""))
    assert(mutantFalse.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (originalValue._1, false)
      }"""))

    // Mutant mapTotrue
    assert(mutantTrue.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantTrue.original == original)
    assert(mutantTrue.mutated != original)

    assert(mutantTrue.mutated.id == mutantTrue.original.id)
    assert(mutantTrue.mutated.edges == mutantTrue.original.edges)

    assert(mutantTrue.mutated.name != mutantTrue.original.name)
    assert(mutantTrue.mutated.name == "mapTo(originalValue._1, true)")

    assert(mutantTrue.mutated.source != mutantTrue.original.source)
    assert(mutantTrue.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => (x.toInt, !x.isEmpty))"""))
    assert(mutantTrue.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (originalValue._1, true)
      })"""))

    assert(mutantTrue.mutated.params.size == 1)
    assert(mutantTrue.original.params.size == 1)
    assert(!mutantTrue.mutated.params(0).isEqual(mutantFalse.original.params(0)))
    assert(mutantTrue.original.params(0).isEqual(q"""(x: String) => (x.toInt, !x.isEmpty)"""))
    assert(mutantTrue.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (originalValue._1, true)
      }"""))

    // Mutant mapTo!originalValue
    assert(mutantNegation.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantNegation.original == original)
    assert(mutantNegation.mutated != original)

    assert(mutantNegation.mutated.id == mutantNegation.original.id)
    assert(mutantNegation.mutated.edges == mutantNegation.original.edges)

    assert(mutantNegation.mutated.name != mutantNegation.original.name)
    assert(mutantNegation.mutated.name == "mapTo(originalValue._1, !originalValue._2)")

    assert(mutantNegation.mutated.source != mutantNegation.original.source)
    assert(mutantNegation.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => (x.toInt, !x.isEmpty))"""))
    assert(mutantNegation.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (originalValue._1, !originalValue._2)
      })"""))

    assert(mutantNegation.mutated.params.size == 1)
    assert(mutantNegation.original.params.size == 1)
    assert(!mutantNegation.mutated.params(0).isEqual(mutantFalse.original.params(0)))
    assert(mutantNegation.original.params(0).isEqual(q"""(x: String) => (x.toInt, !x.isEmpty)"""))
    assert(mutantNegation.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => (x.toInt, !x.isEmpty)
        val originalValue = originalFunction(inputParameter)
        (originalValue._1, !originalValue._2)
      }"""))
  }
  
  test("Test Case 16 - Map to Other Type (General)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        case class Person(name: String)
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => Person(x))
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(ClassType("Person")))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDMappingTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDMappingTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutantNull = mutants(0)

    // Mutant mapTo""
    assert(mutantNull.mutationOperator == MutationOperatorsEnum.MTR)

    assert(mutantNull.original == original)
    assert(mutantNull.mutated != original)

    assert(mutantNull.mutated.id == mutantNull.original.id)
    assert(mutantNull.mutated.edges == mutantNull.original.edges)

    assert(mutantNull.mutated.name != mutantNull.original.name)
    assert(mutantNull.mutated.name == "mapTonull")

    assert(mutantNull.mutated.source != mutantNull.original.source)
    assert(mutantNull.original.source.isEqual(q"""val rdd2 = rdd1.map((x: String) => Person(x))"""))
    assert(mutantNull.mutated.source.isEqual(q"""val rdd2 = rdd1.map((inputParameter: String) => {
        val originalFunction = (x: String) => Person(x)
        val originalValue = originalFunction(inputParameter)
        null
      })"""))

    assert(mutantNull.mutated.params.size == 1)
    assert(mutantNull.original.params.size == 1)
    assert(!mutantNull.mutated.params(0).isEqual(mutantNull.original.params(0)))
    assert(mutantNull.original.params(0).isEqual(q"""(x: String) => Person(x)"""))
    assert(mutantNull.mutated.params(0).isEqual(q"""(inputParameter: String) => {
        val originalFunction = (x: String) => Person(x)
        val originalValue = originalFunction(inputParameter)
        null
      }"""))
  }

}
