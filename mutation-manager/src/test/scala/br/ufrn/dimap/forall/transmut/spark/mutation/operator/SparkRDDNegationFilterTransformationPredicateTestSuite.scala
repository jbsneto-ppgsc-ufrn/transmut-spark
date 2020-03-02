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

class SparkRDDNegationFilterTransformationPredicateTestSuite extends FunSuite {

  test("Test Case 1 - Applicable Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.filter((a: Int) => a < 100)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDNegationFilterTransformationPredicate.isApplicable(original))

    val mutants = SparkRDDNegationFilterTransformationPredicate.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head
    
    assert(mutant.mutationOperator == MutationOperatorsEnum.NFTP)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "filterNot")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.filter((a: Int) => a < 100)"))
    assert(mutant.mutated.source.isEqual(q"""val rdd2 = rdd1.filter((inputParameter: Int) => {
        val originalFunction = ((a: Int) => a < 100)(_)
        val originalValue = originalFunction(inputParameter)
        !originalValue
      })"""))
      
    assert(mutant.mutated.params.size == 1)
    assert(mutant.original.params.size == 1)
    assert(!mutant.mutated.params(0).isEqual(mutant.original.params(0)))
    assert(mutant.original.params(0).isEqual(q"""(a: Int) => a < 100"""))
    assert(mutant.mutated.params(0).isEqual(q"""(inputParameter: Int) => {
        val originalFunction = ((a: Int) => a < 100)(_)
        val originalValue = originalFunction(inputParameter)
        !originalValue
      }"""))  
  }

  test("Test Case 2 - Not Applicable Transformation") {

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

    assert(!SparkRDDNegationFilterTransformationPredicate.isApplicable(original))

    val mutants = SparkRDDNegationFilterTransformationPredicate.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)
  }

}