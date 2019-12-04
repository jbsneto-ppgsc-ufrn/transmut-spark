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

class SparkRDDSetTransformationReplacementTestSuite extends FunSuite {

  test("Test Case 1 - Not Applicable Binary Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]) : RDD[(Long, (Double, String))] = {
          val rdd3 = rdd1.join(rdd2)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.String))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(!SparkRDDSetTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDSetTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)
  }

  test("Test Case 2 - Not Applicable Unary Transformation") {

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

    assert(!SparkRDDSetTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDSetTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)
  }

  test("Test Case 3 - Union Transformation") {

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

    assert(SparkRDDSetTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDSetTransformationReplacement.generateMutants(original, idGenerator)
    
    assert(mutants.size == 5)
    
    val mutantIntersection = mutants(0)
    val mutantSubtract = mutants(1)
    val mutantFirstDataset = mutants(2)
    val mutantSecondDataset = mutants(3)
    val mutantCommutative = mutants(4)

    // Intersection Mutant
    assert(mutantIntersection.original == original)
    assert(mutantIntersection.mutated != original)

    assert(mutantIntersection.mutated.id == mutantIntersection.original.id)
    assert(mutantIntersection.mutated.edges == mutantIntersection.original.edges)

    assert(mutantIntersection.mutated.name != mutantIntersection.original.name)
    assert(mutantIntersection.mutated.name == "intersection")

    assert(mutantIntersection.mutated.source != mutantIntersection.original.source)
    assert(mutantIntersection.original.source.isEqual(q"val rdd3 = rdd1.union(rdd2)"))
    assert(mutantIntersection.mutated.source.isEqual(q"val rdd3 = rdd1.intersection(rdd2)"))

    assert(mutantIntersection.mutated.params == mutantIntersection.original.params)
    
    // Subtract Mutant
    assert(mutantSubtract.original == original)
    assert(mutantSubtract.mutated != original)

    assert(mutantSubtract.mutated.id == mutantSubtract.original.id)
    assert(mutantSubtract.mutated.edges == mutantSubtract.original.edges)

    assert(mutantSubtract.mutated.name != mutantSubtract.original.name)
    assert(mutantSubtract.mutated.name == "subtract")

    assert(mutantSubtract.mutated.source != mutantSubtract.original.source)
    assert(mutantSubtract.original.source.isEqual(q"val rdd3 = rdd1.union(rdd2)"))
    assert(mutantSubtract.mutated.source.isEqual(q"val rdd3 = rdd1.subtract(rdd2)"))

    assert(mutantSubtract.mutated.params == mutantSubtract.original.params)
    
    // First Dataset Mutant
    assert(mutantFirstDataset.original == original)
    assert(mutantFirstDataset.mutated != original)

    assert(mutantFirstDataset.mutated.id == mutantFirstDataset.original.id)
    assert(mutantFirstDataset.mutated.edges == mutantFirstDataset.original.edges)

    assert(mutantFirstDataset.mutated.name != mutantFirstDataset.original.name)
    assert(mutantFirstDataset.mutated.name == "firstDataset")

    assert(mutantFirstDataset.mutated.source != mutantFirstDataset.original.source)
    assert(mutantFirstDataset.original.source.isEqual(q"val rdd3 = rdd1.union(rdd2)"))
    assert(mutantFirstDataset.mutated.source.isEqual(q"val rdd3 = rdd1"))

    assert(mutantFirstDataset.mutated.params != mutantFirstDataset.original.params)    
    assert(mutantFirstDataset.mutated.params.isEmpty)
    
    // Second Dataset Mutant
    assert(mutantSecondDataset.original == original)
    assert(mutantSecondDataset.mutated != original)

    assert(mutantSecondDataset.mutated.id == mutantSecondDataset.original.id)
    assert(mutantSecondDataset.mutated.edges == mutantSecondDataset.original.edges)

    assert(mutantSecondDataset.mutated.name != mutantSecondDataset.original.name)
    assert(mutantSecondDataset.mutated.name == "secondDataset")

    assert(mutantSecondDataset.mutated.source != mutantSecondDataset.original.source)
    assert(mutantSecondDataset.original.source.isEqual(q"val rdd3 = rdd1.union(rdd2)"))
    assert(mutantSecondDataset.mutated.source.isEqual(q"val rdd3 = rdd2"))

    assert(mutantSecondDataset.mutated.params != mutantSecondDataset.original.params)    
    assert(mutantSecondDataset.mutated.params.isEmpty)

    // Commutative Mutant
    assert(mutantCommutative.original == original)
    assert(mutantCommutative.mutated != original)

    assert(mutantCommutative.mutated.id == mutantCommutative.original.id)
    assert(mutantCommutative.mutated.edges == mutantCommutative.original.edges)

    assert(mutantCommutative.mutated.name != mutantCommutative.original.name)
    assert(mutantCommutative.mutated.name == (mutantCommutative.original.name + "Commutative"))

    assert(mutantCommutative.mutated.source != mutantCommutative.original.source)
    assert(mutantCommutative.original.source.isEqual(q"val rdd3 = rdd1.union(rdd2)"))
    assert(mutantCommutative.mutated.source.isEqual(q"val rdd3 = rdd2.union(rdd1)"))

    assert(mutantCommutative.mutated.params != mutantCommutative.original.params)
    assert(mutantCommutative.mutated.params.size == 1)
    assert(mutantCommutative.mutated.params.head.isEqual(q"rdd1"))
    
  }
  
  test("Test Case 4 - Intersection Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.SparkContext
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.intersection(rdd2)
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

    assert(SparkRDDSetTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDSetTransformationReplacement.generateMutants(original, idGenerator)
    
    assert(mutants.size == 5)
    
    val mutantUnion = mutants(0)
    val mutantSubtract = mutants(1)
    val mutantFirstDataset = mutants(2)
    val mutantSecondDataset = mutants(3)
    val mutantCommutative = mutants(4)

    // Union Mutant
    assert(mutantUnion.original == original)
    assert(mutantUnion.mutated != original)

    assert(mutantUnion.mutated.id == mutantUnion.original.id)
    assert(mutantUnion.mutated.edges == mutantUnion.original.edges)

    assert(mutantUnion.mutated.name != mutantUnion.original.name)
    assert(mutantUnion.mutated.name == "union")

    assert(mutantUnion.mutated.source != mutantUnion.original.source)
    assert(mutantUnion.original.source.isEqual(q"val rdd3 = rdd1.intersection(rdd2)"))
    assert(mutantUnion.mutated.source.isEqual(q"val rdd3 = rdd1.union(rdd2)"))

    assert(mutantUnion.mutated.params == mutantUnion.original.params)
    
    // Subtract Mutant
    assert(mutantSubtract.original == original)
    assert(mutantSubtract.mutated != original)

    assert(mutantSubtract.mutated.id == mutantSubtract.original.id)
    assert(mutantSubtract.mutated.edges == mutantSubtract.original.edges)

    assert(mutantSubtract.mutated.name != mutantSubtract.original.name)
    assert(mutantSubtract.mutated.name == "subtract")

    assert(mutantSubtract.mutated.source != mutantSubtract.original.source)
    assert(mutantSubtract.original.source.isEqual(q"val rdd3 = rdd1.intersection(rdd2)"))
    assert(mutantSubtract.mutated.source.isEqual(q"val rdd3 = rdd1.subtract(rdd2)"))

    assert(mutantSubtract.mutated.params == mutantSubtract.original.params)
    
    // First Dataset Mutant
    assert(mutantFirstDataset.original == original)
    assert(mutantFirstDataset.mutated != original)

    assert(mutantFirstDataset.mutated.id == mutantFirstDataset.original.id)
    assert(mutantFirstDataset.mutated.edges == mutantFirstDataset.original.edges)

    assert(mutantFirstDataset.mutated.name != mutantFirstDataset.original.name)
    assert(mutantFirstDataset.mutated.name == "firstDataset")

    assert(mutantFirstDataset.mutated.source != mutantFirstDataset.original.source)
    assert(mutantFirstDataset.original.source.isEqual(q"val rdd3 = rdd1.intersection(rdd2)"))
    assert(mutantFirstDataset.mutated.source.isEqual(q"val rdd3 = rdd1"))

    assert(mutantFirstDataset.mutated.params != mutantFirstDataset.original.params)    
    assert(mutantFirstDataset.mutated.params.isEmpty)
    
    // Second Dataset Mutant
    assert(mutantSecondDataset.original == original)
    assert(mutantSecondDataset.mutated != original)

    assert(mutantSecondDataset.mutated.id == mutantSecondDataset.original.id)
    assert(mutantSecondDataset.mutated.edges == mutantSecondDataset.original.edges)

    assert(mutantSecondDataset.mutated.name != mutantSecondDataset.original.name)
    assert(mutantSecondDataset.mutated.name == "secondDataset")

    assert(mutantSecondDataset.mutated.source != mutantSecondDataset.original.source)
    assert(mutantSecondDataset.original.source.isEqual(q"val rdd3 = rdd1.intersection(rdd2)"))
    assert(mutantSecondDataset.mutated.source.isEqual(q"val rdd3 = rdd2"))

    assert(mutantSecondDataset.mutated.params != mutantSecondDataset.original.params)    
    assert(mutantSecondDataset.mutated.params.isEmpty)

    // Commutative Mutant
    assert(mutantCommutative.original == original)
    assert(mutantCommutative.mutated != original)

    assert(mutantCommutative.mutated.id == mutantCommutative.original.id)
    assert(mutantCommutative.mutated.edges == mutantCommutative.original.edges)

    assert(mutantCommutative.mutated.name != mutantCommutative.original.name)
    assert(mutantCommutative.mutated.name == (mutantCommutative.original.name + "Commutative"))

    assert(mutantCommutative.mutated.source != mutantCommutative.original.source)
    assert(mutantCommutative.original.source.isEqual(q"val rdd3 = rdd1.intersection(rdd2)"))
    assert(mutantCommutative.mutated.source.isEqual(q"val rdd3 = rdd2.intersection(rdd1)"))

    assert(mutantCommutative.mutated.params != mutantCommutative.original.params)
    assert(mutantCommutative.mutated.params.size == 1)
    assert(mutantCommutative.mutated.params.head.isEqual(q"rdd1"))
    
  }
  
  test("Test Case 5 - Subtract Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.SparkContext
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.subtract(rdd2)
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

    assert(SparkRDDSetTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDSetTransformationReplacement.generateMutants(original, idGenerator)
    
    assert(mutants.size == 5)
    
    val mutantUnion = mutants(0)
    val mutantIntersection = mutants(1)
    val mutantFirstDataset = mutants(2)
    val mutantSecondDataset = mutants(3)
    val mutantCommutative = mutants(4)

    // Intersection Mutant
    assert(mutantUnion.original == original)
    assert(mutantUnion.mutated != original)

    assert(mutantUnion.mutated.id == mutantUnion.original.id)
    assert(mutantUnion.mutated.edges == mutantUnion.original.edges)

    assert(mutantUnion.mutated.name != mutantUnion.original.name)
    assert(mutantUnion.mutated.name == "union")

    assert(mutantUnion.mutated.source != mutantUnion.original.source)
    assert(mutantUnion.original.source.isEqual(q"val rdd3 = rdd1.subtract(rdd2)"))
    assert(mutantUnion.mutated.source.isEqual(q"val rdd3 = rdd1.union(rdd2)"))

    assert(mutantUnion.mutated.params == mutantUnion.original.params)
    
    // Intersection Mutant
    assert(mutantIntersection.original == original)
    assert(mutantIntersection.mutated != original)

    assert(mutantIntersection.mutated.id == mutantIntersection.original.id)
    assert(mutantIntersection.mutated.edges == mutantIntersection.original.edges)

    assert(mutantIntersection.mutated.name != mutantIntersection.original.name)
    assert(mutantIntersection.mutated.name == "intersection")

    assert(mutantIntersection.mutated.source != mutantIntersection.original.source)
    assert(mutantIntersection.original.source.isEqual(q"val rdd3 = rdd1.subtract(rdd2)"))
    assert(mutantIntersection.mutated.source.isEqual(q"val rdd3 = rdd1.intersection(rdd2)"))

    assert(mutantIntersection.mutated.params == mutantIntersection.original.params)
    
    // First Dataset Mutant
    assert(mutantFirstDataset.original == original)
    assert(mutantFirstDataset.mutated != original)

    assert(mutantFirstDataset.mutated.id == mutantFirstDataset.original.id)
    assert(mutantFirstDataset.mutated.edges == mutantFirstDataset.original.edges)

    assert(mutantFirstDataset.mutated.name != mutantFirstDataset.original.name)
    assert(mutantFirstDataset.mutated.name == "firstDataset")

    assert(mutantFirstDataset.mutated.source != mutantFirstDataset.original.source)
    assert(mutantFirstDataset.original.source.isEqual(q"val rdd3 = rdd1.subtract(rdd2)"))
    assert(mutantFirstDataset.mutated.source.isEqual(q"val rdd3 = rdd1"))

    assert(mutantFirstDataset.mutated.params != mutantFirstDataset.original.params)    
    assert(mutantFirstDataset.mutated.params.isEmpty)
    
    // Second Dataset Mutant
    assert(mutantSecondDataset.original == original)
    assert(mutantSecondDataset.mutated != original)

    assert(mutantSecondDataset.mutated.id == mutantSecondDataset.original.id)
    assert(mutantSecondDataset.mutated.edges == mutantSecondDataset.original.edges)

    assert(mutantSecondDataset.mutated.name != mutantSecondDataset.original.name)
    assert(mutantSecondDataset.mutated.name == "secondDataset")

    assert(mutantSecondDataset.mutated.source != mutantSecondDataset.original.source)
    assert(mutantSecondDataset.original.source.isEqual(q"val rdd3 = rdd1.subtract(rdd2)"))
    assert(mutantSecondDataset.mutated.source.isEqual(q"val rdd3 = rdd2"))

    assert(mutantSecondDataset.mutated.params != mutantSecondDataset.original.params)    
    assert(mutantSecondDataset.mutated.params.isEmpty)

    // Commutative Mutant
    assert(mutantCommutative.original == original)
    assert(mutantCommutative.mutated != original)

    assert(mutantCommutative.mutated.id == mutantCommutative.original.id)
    assert(mutantCommutative.mutated.edges == mutantCommutative.original.edges)

    assert(mutantCommutative.mutated.name != mutantCommutative.original.name)
    assert(mutantCommutative.mutated.name == (mutantCommutative.original.name + "Commutative"))

    assert(mutantCommutative.mutated.source != mutantCommutative.original.source)
    assert(mutantCommutative.original.source.isEqual(q"val rdd3 = rdd1.subtract(rdd2)"))
    assert(mutantCommutative.mutated.source.isEqual(q"val rdd3 = rdd2.subtract(rdd1)"))

    assert(mutantCommutative.mutated.params != mutantCommutative.original.params)
    assert(mutantCommutative.mutated.params.size == 1)
    assert(mutantCommutative.mutated.params.head.isEqual(q"rdd1")) 
  }
}