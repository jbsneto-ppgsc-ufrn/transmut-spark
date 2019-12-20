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
import br.ufrn.dimap.forall.transmut.model.TupleType
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDBinaryTransformation
import br.ufrn.dimap.forall.transmut.model.DirectionsEnum
import br.ufrn.dimap.forall.transmut.model.ReferencesTypeEnum
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

class SparkRDDBinaryTransformationSwapTestSuite extends FunSuite {

  test("Test Case 1 - Not Applicable Binary Transformations") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, Double)]) : RDD[(Long, (Double, Double))] = {
          val rdd3 = rdd1.join(rdd2)
          val rdd4 = rdd2.join(rdd1)
          val rdd5 = rdd3.intersection(rdd4)
          rdd5
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.Double)))))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.Double)))))))
    refenceTypes += ("rdd5" -> ValReference("rdd5", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.Double)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val originals = programSource.programs.head.transformations

    assert(!SparkRDDBinaryTransformationSwap.isApplicable(originals))

    val mutants = SparkRDDBinaryTransformationSwap.generateMutants(originals, idGenerator)

    assert(mutants.isEmpty)
  }

  test("Test Case 2 - Not Applicable Unary Transformations") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map(a => a + 1)
          val rdd3 = rdd2.filter(a => a % 2 == 0)
          val rdd4 = rdd3.distinct
          rdd4
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val originals = programSource.programs.head.transformations

    assert(!SparkRDDBinaryTransformationSwap.isApplicable(originals))

    val mutants = SparkRDDBinaryTransformationSwap.generateMutants(originals, idGenerator)

    assert(mutants.isEmpty)
  }

  test("Test Case 3 - Three Applicable Binary Transformations") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.SparkContext
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.subtract(rdd2)
          val rdd4 = rdd1.intersection(rdd2)
          val rdd5 = rdd3.union(rdd4)
          rdd5
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd5" -> ValReference("rdd5", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val originals = program.transformations

    assert(SparkRDDBinaryTransformationSwap.isApplicable(originals))

    val mutants = SparkRDDBinaryTransformationSwap.generateMutants(originals, idGenerator)
    
    assert(mutants.size == 3)

    val mutant1 = mutants(0)
    val mutant2 = mutants(1)
    val mutant3 = mutants(2)

    // Mutant 1
    assert(mutant1.mutationOperator == MutationOperatorsEnum.BTS)

    assert(mutant1.original.size == 2)
    assert(originals.contains(mutant1.original(0)))
    assert(originals.contains(mutant1.original(1)))
    assert(mutant1.original(0) != mutant1.original(1))

    assert(mutant1.mutated.size == 2)
    assert(!originals.contains(mutant1.mutated(0)))
    assert(!originals.contains(mutant1.mutated(1)))

    assert(mutant1.mutated(0).id == mutant1.original(0).id)
    assert(mutant1.mutated(0).id != mutant1.original(1).id)
    assert(mutant1.mutated(1).id != mutant1.original(0).id)
    assert(mutant1.mutated(1).id == mutant1.original(1).id)
    assert(mutant1.mutated(0).edges == mutant1.original(0).edges)
    assert(mutant1.mutated(0).edges != mutant1.original(1).edges)
    assert(mutant1.mutated(1).edges != mutant1.original(0).edges)
    assert(mutant1.mutated(1).edges == mutant1.original(1).edges)

    assert(mutant1.mutated(0).name != mutant1.original(0).name)
    assert(mutant1.mutated(0).name != mutant1.original(1).name)
    assert(mutant1.mutated(1).name != mutant1.original(0).name)
    assert(mutant1.mutated(1).name != mutant1.original(1).name)
    assert(mutant1.mutated(0).name == "subtractTointersection")
    assert(mutant1.mutated(1).name == "intersectionTosubtract")

    assert(mutant1.mutated(0).source != mutant1.original(0).source)
    assert(mutant1.mutated(0).source != mutant1.original(1).source)
    assert(mutant1.mutated(1).source != mutant1.original(0).source)
    assert(mutant1.mutated(1).source != mutant1.original(1).source)
    assert(mutant1.original(0).source.isEqual(q"val rdd3 = rdd1.subtract(rdd2)"))
    assert(mutant1.original(1).source.isEqual(q"val rdd4 = rdd1.intersection(rdd2)"))
    assert(mutant1.mutated(0).source.isEqual(q"val rdd3 = rdd1.intersection(rdd2)"))
    assert(mutant1.mutated(1).source.isEqual(q"val rdd4 = rdd1.subtract(rdd2)"))

    assert(mutant1.mutated(0).params == mutant1.original(0).params)
    assert(mutant1.mutated(0).params != mutant1.original(1).params)
    assert(mutant1.mutated(1).params != mutant1.original(0).params)
    assert(mutant1.mutated(1).params == mutant1.original(1).params)

    // Mutant 2
    assert(mutant2.mutationOperator == MutationOperatorsEnum.BTS)

    assert(mutant2.original.size == 2)
    assert(originals.contains(mutant2.original(0)))
    assert(originals.contains(mutant2.original(1)))
    assert(mutant2.original(0) != mutant2.original(1))

    assert(mutant2.mutated.size == 2)
    assert(!originals.contains(mutant2.mutated(0)))
    assert(!originals.contains(mutant2.mutated(1)))

    assert(mutant2.mutated(0).id == mutant2.original(0).id)
    assert(mutant2.mutated(0).id != mutant2.original(1).id)
    assert(mutant2.mutated(1).id != mutant2.original(0).id)
    assert(mutant2.mutated(1).id == mutant2.original(1).id)
    assert(mutant2.mutated(0).edges == mutant2.original(0).edges)
    assert(mutant2.mutated(0).edges != mutant2.original(1).edges)
    assert(mutant2.mutated(1).edges != mutant2.original(0).edges)
    assert(mutant2.mutated(1).edges == mutant2.original(1).edges)

    assert(mutant2.mutated(0).name != mutant2.original(0).name)
    assert(mutant2.mutated(0).name != mutant2.original(1).name)
    assert(mutant2.mutated(1).name != mutant2.original(0).name)
    assert(mutant2.mutated(1).name != mutant2.original(1).name)
    assert(mutant2.mutated(0).name == "subtractTounion")
    assert(mutant2.mutated(1).name == "unionTosubtract")

    assert(mutant2.mutated(0).source != mutant2.original(0).source)
    assert(mutant2.mutated(0).source != mutant2.original(1).source)
    assert(mutant2.mutated(1).source != mutant2.original(0).source)
    assert(mutant2.mutated(1).source != mutant2.original(1).source)
    assert(mutant2.original(0).source.isEqual(q"val rdd3 = rdd1.subtract(rdd2)"))
    assert(mutant2.original(1).source.isEqual(q"val rdd5 = rdd3.union(rdd4)"))
    assert(mutant2.mutated(0).source.isEqual(q"val rdd3 = rdd1.union(rdd2)"))
    assert(mutant2.mutated(1).source.isEqual(q"val rdd5 = rdd3.subtract(rdd4)"))

    assert(mutant2.mutated(0).params == mutant2.original(0).params)
    assert(mutant2.mutated(0).params != mutant2.original(1).params)
    assert(mutant2.mutated(1).params != mutant2.original(0).params)
    assert(mutant2.mutated(1).params == mutant2.original(1).params)

    // Mutant 3
    assert(mutant3.mutationOperator == MutationOperatorsEnum.BTS)

    assert(mutant3.original.size == 2)
    assert(originals.contains(mutant3.original(0)))
    assert(originals.contains(mutant3.original(1)))
    assert(mutant3.original(0) != mutant3.original(1))

    assert(mutant3.mutated.size == 2)
    assert(!originals.contains(mutant3.mutated(0)))
    assert(!originals.contains(mutant3.mutated(1)))

    assert(mutant3.mutated(0).id == mutant3.original(0).id)
    assert(mutant3.mutated(0).id != mutant3.original(1).id)
    assert(mutant3.mutated(1).id != mutant3.original(0).id)
    assert(mutant3.mutated(1).id == mutant3.original(1).id)
    assert(mutant3.mutated(0).edges == mutant3.original(0).edges)
    assert(mutant3.mutated(0).edges != mutant3.original(1).edges)
    assert(mutant3.mutated(1).edges != mutant3.original(0).edges)
    assert(mutant3.mutated(1).edges == mutant3.original(1).edges)

    assert(mutant3.mutated(0).name != mutant3.original(0).name)
    assert(mutant3.mutated(0).name != mutant3.original(1).name)
    assert(mutant3.mutated(1).name != mutant3.original(0).name)
    assert(mutant3.mutated(1).name != mutant3.original(1).name)
    assert(mutant3.mutated(0).name == "intersectionTounion")
    assert(mutant3.mutated(1).name == "unionTointersection")

    assert(mutant3.mutated(0).source != mutant3.original(0).source)
    assert(mutant3.mutated(0).source != mutant3.original(1).source)
    assert(mutant3.mutated(1).source != mutant3.original(0).source)
    assert(mutant3.mutated(1).source != mutant3.original(1).source)
    assert(mutant3.original(0).source.isEqual(q"val rdd4 = rdd1.intersection(rdd2)"))
    assert(mutant3.original(1).source.isEqual(q"val rdd5 = rdd3.union(rdd4)"))
    assert(mutant3.mutated(0).source.isEqual(q"val rdd4 = rdd1.union(rdd2)"))
    assert(mutant3.mutated(1).source.isEqual(q"val rdd5 = rdd3.intersection(rdd4)"))

    assert(mutant3.mutated(0).params == mutant3.original(0).params)
    assert(mutant3.mutated(0).params != mutant3.original(1).params)
    assert(mutant3.mutated(1).params != mutant3.original(0).params)
    assert(mutant3.mutated(1).params == mutant3.original(1).params)
    
  }
  
  test("Test Case 4 - Two Applicable Binary Transformations and One Not Applicable Unary Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.SparkContext
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.subtract(rdd2)
          val rdd4 = rdd3.union(rdd2)
          val rdd5 = rdd4.distinct
          rdd5
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd5" -> ValReference("rdd5", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val originals = program.transformations

    assert(SparkRDDBinaryTransformationSwap.isApplicable(originals))

    val mutants = SparkRDDBinaryTransformationSwap.generateMutants(originals, idGenerator)
    
    assert(mutants.size == 1)

    val mutant1 = mutants(0)

    // Mutant 1
    assert(mutant1.mutationOperator == MutationOperatorsEnum.BTS)

    assert(mutant1.original.size == 2)
    assert(originals.contains(mutant1.original(0)))
    assert(originals.contains(mutant1.original(1)))
    assert(mutant1.original(0) != mutant1.original(1))

    assert(mutant1.mutated.size == 2)
    assert(!originals.contains(mutant1.mutated(0)))
    assert(!originals.contains(mutant1.mutated(1)))

    assert(mutant1.mutated(0).id == mutant1.original(0).id)
    assert(mutant1.mutated(0).id != mutant1.original(1).id)
    assert(mutant1.mutated(1).id != mutant1.original(0).id)
    assert(mutant1.mutated(1).id == mutant1.original(1).id)
    assert(mutant1.mutated(0).edges == mutant1.original(0).edges)
    assert(mutant1.mutated(0).edges != mutant1.original(1).edges)
    assert(mutant1.mutated(1).edges != mutant1.original(0).edges)
    assert(mutant1.mutated(1).edges == mutant1.original(1).edges)

    assert(mutant1.mutated(0).name != mutant1.original(0).name)
    assert(mutant1.mutated(0).name != mutant1.original(1).name)
    assert(mutant1.mutated(1).name != mutant1.original(0).name)
    assert(mutant1.mutated(1).name != mutant1.original(1).name)
    assert(mutant1.mutated(0).name == "subtractTounion")
    assert(mutant1.mutated(1).name == "unionTosubtract")

    assert(mutant1.mutated(0).source != mutant1.original(0).source)
    assert(mutant1.mutated(0).source != mutant1.original(1).source)
    assert(mutant1.mutated(1).source != mutant1.original(0).source)
    assert(mutant1.mutated(1).source != mutant1.original(1).source)
    assert(mutant1.original(0).source.isEqual(q"val rdd3 = rdd1.subtract(rdd2)"))
    assert(mutant1.original(1).source.isEqual(q"val rdd4 = rdd3.union(rdd2)"))
    assert(mutant1.mutated(0).source.isEqual(q"val rdd3 = rdd1.union(rdd2)"))
    assert(mutant1.mutated(1).source.isEqual(q"val rdd4 = rdd3.subtract(rdd2)"))

    assert(mutant1.mutated(0).params == mutant1.original(0).params)
    assert(mutant1.mutated(0).params != mutant1.original(1).params)
    assert(mutant1.mutated(1).params != mutant1.original(0).params)
    assert(mutant1.mutated(1).params == mutant1.original(1).params)
  }
  
}