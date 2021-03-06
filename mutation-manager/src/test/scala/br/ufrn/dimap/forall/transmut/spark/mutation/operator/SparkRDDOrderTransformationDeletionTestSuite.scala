package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import org.scalatest.FunSuite
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.model.Reference
import scala.meta._
import scala.meta.contrib._
import br.ufrn.dimap.forall.transmut.model.BaseTypesEnum
import br.ufrn.dimap.forall.transmut.model.ValReference
import br.ufrn.dimap.forall.transmut.model.ParameterReference
import br.ufrn.dimap.forall.transmut.model.BaseType
import br.ufrn.dimap.forall.transmut.model.ParameterizedType
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

class SparkRDDOrderTransformationDeletionTestSuite extends FunSuite {

  test("Test Case 1 - Applicable sortBy Transformation with parameter") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortBy(x => x)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationDeletion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationDeletion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head
    
    assert(mutant.mutationOperator == MutationOperatorsEnum.OTD)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "identity")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortBy(x => x)"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1"))

    assert(mutant.original.params.size == 1)
    assert(mutant.original.params.head.isEqual(q"x => x"))
    assert(mutant.mutated.params.isEmpty)

  }
  
  test("Test Case 2 - Applicable sortByKey Transformation without parentheses") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortByKey
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationDeletion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationDeletion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head
    
    assert(mutant.mutationOperator == MutationOperatorsEnum.OTD)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "identity")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortByKey"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1"))

    assert(mutant.original.params.isEmpty)
    assert(mutant.mutated.params.isEmpty)

  }
  
  test("Test Case 3 - Applicable sortByKey Transformation with parameter") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortByKey(false)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationDeletion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationDeletion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head
    
    assert(mutant.mutationOperator == MutationOperatorsEnum.OTD)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "identity")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortByKey(false)"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1"))

    assert(mutant.mutated.params != mutant.original.params)
    assert(mutant.mutated.params.isEmpty)

  }
  
  test("Test Case 4 - Applicable sortByKey Transformation with parentheses and without parameters") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortByKey()
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationDeletion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationDeletion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head
    
    assert(mutant.mutationOperator == MutationOperatorsEnum.OTD)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "identity")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortByKey()"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1"))

    assert(mutant.original.params.isEmpty)
    assert(mutant.mutated.params.isEmpty)

  }

  test("Test Case 5 - Not Applicable Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map(a => a + 1)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(!SparkRDDOrderTransformationDeletion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationDeletion.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)
  }

}