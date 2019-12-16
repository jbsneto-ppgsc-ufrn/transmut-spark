package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._
import scala.meta.contrib._

import org.scalatest.FunSuite

import br.ufrn.dimap.forall.transmut.model.Reference
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.model.BaseTypesEnum
import br.ufrn.dimap.forall.transmut.model.ValReference
import br.ufrn.dimap.forall.transmut.model.ParameterReference
import br.ufrn.dimap.forall.transmut.model.BaseType
import br.ufrn.dimap.forall.transmut.model.ParameterizedType
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

class SparkRDDDistinctTransformationInsertionTestSuite extends FunSuite {

  test("Test Case 1 - Not Applicable Transformation (distinct)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.distinct()
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head
    
    assert(!SparkRDDDistinctTransformationInsertion.isApplicable(original))

    val mutants = SparkRDDDistinctTransformationInsertion.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)

  }
  
  test("Test Case 2 - Not Applicable Transformation (Action)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val result = rdd1.reduce((a, b) => a + b)
          result
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("result" -> ValReference("result", BaseType(BaseTypesEnum.Int)))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head
    
    assert(!SparkRDDDistinctTransformationInsertion.isApplicable(original))

    val mutants = SparkRDDDistinctTransformationInsertion.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)

  }

  test("Test Case 3 - Applicable Unary Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map((a: Int) => a + 1)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDDistinctTransformationInsertion.isApplicable(original))

    val mutants = SparkRDDDistinctTransformationInsertion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)
    
    val mutant = mutants.head
    
    assert(mutant.mutationOperator == MutationOperatorsEnum.DTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "mapDistinct")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.map((a: Int) => a + 1)"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.map((a: Int) => a + 1).distinct()"))

    assert(mutant.mutated.params == mutant.original.params)
  }
  
  test("Test Case 4 - Applicable Binary Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.SparkContext
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

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val original = program.transformations.head

    assert(SparkRDDDistinctTransformationInsertion.isApplicable(original))

    val mutants = SparkRDDDistinctTransformationInsertion.generateMutants(original, idGenerator)
    
    assert(mutants.size == 1)
    
    val mutant = mutants(0)
    
    assert(mutant.mutationOperator == MutationOperatorsEnum.DTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "unionDistinct")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd3 = rdd1.union(rdd2)"))
    assert(mutant.mutated.source.isEqual(q"val rdd3 = rdd1.union(rdd2).distinct()"))

    assert(mutant.mutated.params == mutant.original.params)
    
  }
  
  test("Test Case 5 - Applicable Not Supported Transformation") {

    val idGenerator = LongIdGenerator.generator
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.cache
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDDistinctTransformationInsertion.isApplicable(original))

    val mutants = SparkRDDDistinctTransformationInsertion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)
    
    val mutant = mutants.head
    
    assert(mutant.mutationOperator == MutationOperatorsEnum.DTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "cacheDistinct")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.cache"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.cache.distinct()"))

    assert(mutant.mutated.params == mutant.original.params)
  }

}