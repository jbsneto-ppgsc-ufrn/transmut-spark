package br.ufrn.dimap.forall.transmut.spark.analyzer

import scala.meta._
import scala.meta.Tree
import scala.meta.contrib._

import org.scalatest.FunSuite
import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDUnaryTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDBinaryTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDOperation

class SparkRDDProgramBuilderTestSuite extends FunSuite {

  test("Test Case 1 - Program with parameterized unary transformations using val references") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map(a => a * 2)
          val rdd3 = rdd2.filter(a => a < 100)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    assert(program.datasets.size == 3)

    assert(program.isDatasetByReferenceNameDefined("rdd1"))
    assert(program.datasetByReferenceName("rdd1").get.reference.referenceType == ReferencesTypeEnum.Parameter)
    assert(program.datasetByReferenceName("rdd1").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(program.datasetByReferenceName("rdd1").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd1").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd2"))
    assert(program.datasetByReferenceName("rdd2").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd2").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(!program.datasetByReferenceName("rdd2").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd2").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd3"))
    assert(program.datasetByReferenceName("rdd3").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd3").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(!program.datasetByReferenceName("rdd3").get.isInputDataset)
    assert(program.datasetByReferenceName("rdd3").get.isOutputDataset)

    assert(program.transformations.size == 2)

    assert(program.transformations(0).isInstanceOf[SparkRDDUnaryTransformation])
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].name == "map")
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].params.size == 1)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].params.head.isEqual(q"a => a * 2"))
    assert(!program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].isLoadTransformation)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].inputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].inputDataset.get == program.datasetByReferenceName("rdd1").get)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].outputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].outputDataset.get == program.datasetByReferenceName("rdd2").get)

    assert(program.transformations(1).isInstanceOf[SparkRDDUnaryTransformation])
    assert(program.transformations(1).asInstanceOf[SparkRDDUnaryTransformation].name == "filter")
    assert(program.transformations(1).asInstanceOf[SparkRDDUnaryTransformation].params.size == 1)
    assert(program.transformations(1).asInstanceOf[SparkRDDUnaryTransformation].params.head.isEqual(q"a => a < 100"))
    assert(!program.transformations(1).asInstanceOf[SparkRDDUnaryTransformation].isLoadTransformation)
    assert(program.transformations(1).asInstanceOf[SparkRDDUnaryTransformation].inputDataset.isDefined)
    assert(program.transformations(1).asInstanceOf[SparkRDDUnaryTransformation].inputDataset.get == program.datasetByReferenceName("rdd2").get)
    assert(program.transformations(1).asInstanceOf[SparkRDDUnaryTransformation].outputDataset.isDefined)
    assert(program.transformations(1).asInstanceOf[SparkRDDUnaryTransformation].outputDataset.get == program.datasetByReferenceName("rdd3").get)

    assert(program.edges.size == 4)

    assert(program.edges(0).dataset == program.datasetByReferenceName("rdd1").get)
    assert(program.edges(0).transformation == program.transformations(0))
    assert(program.edges(0).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(1).dataset == program.datasetByReferenceName("rdd2").get)
    assert(program.edges(1).transformation == program.transformations(0))
    assert(program.edges(1).direction == DirectionsEnum.TransformationToDataset)

    assert(program.edges(2).dataset == program.datasetByReferenceName("rdd2").get)
    assert(program.edges(2).transformation == program.transformations(1))
    assert(program.edges(2).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(3).dataset == program.datasetByReferenceName("rdd3").get)
    assert(program.edges(3).transformation == program.transformations(1))
    assert(program.edges(3).direction == DirectionsEnum.TransformationToDataset)

  }

  test("Test Case 2 - Program with a not parameterized unary transformation using var references") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          var rdd2 = rdd1.distinct
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    assert(program.datasets.size == 2)

    assert(program.isDatasetByReferenceNameDefined("rdd1"))
    assert(program.datasetByReferenceName("rdd1").get.reference.referenceType == ReferencesTypeEnum.Parameter)
    assert(program.datasetByReferenceName("rdd1").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(program.datasetByReferenceName("rdd1").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd1").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd2"))
    assert(program.datasetByReferenceName("rdd2").get.reference.referenceType == ReferencesTypeEnum.Var)
    assert(program.datasetByReferenceName("rdd2").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(!program.datasetByReferenceName("rdd2").get.isInputDataset)
    assert(program.datasetByReferenceName("rdd2").get.isOutputDataset)

    assert(program.transformations.size == 1)

    assert(program.transformations(0).isInstanceOf[SparkRDDUnaryTransformation])
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].name == "distinct")
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].params.isEmpty)
    assert(!program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].isLoadTransformation)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].inputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].inputDataset.get == program.datasetByReferenceName("rdd1").get)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].outputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].outputDataset.get == program.datasetByReferenceName("rdd2").get)

    assert(program.edges.size == 2)

    assert(program.edges(0).dataset == program.datasetByReferenceName("rdd1").get)
    assert(program.edges(0).transformation == program.transformations(0))
    assert(program.edges(0).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(1).dataset == program.datasetByReferenceName("rdd2").get)
    assert(program.edges(1).transformation == program.transformations(0))
    assert(program.edges(1).direction == DirectionsEnum.TransformationToDataset)

  }

  test("Test Case 3 - Program with binary transformations (Set Transformations)") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.intersection(rdd2)
          val rdd4 = rdd1.subtract(rdd2)
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

    assert(program.datasets.size == 5)

    assert(program.isDatasetByReferenceNameDefined("rdd1"))
    assert(program.datasetByReferenceName("rdd1").get.reference.referenceType == ReferencesTypeEnum.Parameter)
    assert(program.datasetByReferenceName("rdd1").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(program.datasetByReferenceName("rdd1").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd1").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd2"))
    assert(program.datasetByReferenceName("rdd2").get.reference.referenceType == ReferencesTypeEnum.Parameter)
    assert(program.datasetByReferenceName("rdd2").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(program.datasetByReferenceName("rdd2").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd2").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd3"))
    assert(program.datasetByReferenceName("rdd3").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd3").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(!program.datasetByReferenceName("rdd3").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd3").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd4"))
    assert(program.datasetByReferenceName("rdd4").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd4").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(!program.datasetByReferenceName("rdd4").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd4").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd5"))
    assert(program.datasetByReferenceName("rdd5").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd5").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(!program.datasetByReferenceName("rdd5").get.isInputDataset)
    assert(program.datasetByReferenceName("rdd5").get.isOutputDataset)

    assert(program.transformations.size == 3)

    assert(program.transformations(0).isInstanceOf[SparkRDDBinaryTransformation])
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].name == "intersection")
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].params.size == 1)
    assert(!program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].isLoadTransformation)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].firstInputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].firstInputDataset.get == program.datasetByReferenceName("rdd1").get)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].secondInputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].secondInputDataset.get == program.datasetByReferenceName("rdd2").get)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].outputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].outputDataset.get == program.datasetByReferenceName("rdd3").get)

    assert(program.transformations(1).isInstanceOf[SparkRDDBinaryTransformation])
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].name == "subtract")
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].params.size == 1)
    assert(!program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].isLoadTransformation)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].firstInputDataset.isDefined)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].firstInputDataset.get == program.datasetByReferenceName("rdd1").get)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].secondInputDataset.isDefined)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].secondInputDataset.get == program.datasetByReferenceName("rdd2").get)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].outputDataset.isDefined)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].outputDataset.get == program.datasetByReferenceName("rdd4").get)

    assert(program.transformations(2).isInstanceOf[SparkRDDBinaryTransformation])
    assert(program.transformations(2).asInstanceOf[SparkRDDBinaryTransformation].name == "union")
    assert(program.transformations(2).asInstanceOf[SparkRDDBinaryTransformation].params.size == 1)
    assert(!program.transformations(2).asInstanceOf[SparkRDDBinaryTransformation].isLoadTransformation)
    assert(program.transformations(2).asInstanceOf[SparkRDDBinaryTransformation].firstInputDataset.isDefined)
    assert(program.transformations(2).asInstanceOf[SparkRDDBinaryTransformation].firstInputDataset.get == program.datasetByReferenceName("rdd3").get)
    assert(program.transformations(2).asInstanceOf[SparkRDDBinaryTransformation].secondInputDataset.isDefined)
    assert(program.transformations(2).asInstanceOf[SparkRDDBinaryTransformation].secondInputDataset.get == program.datasetByReferenceName("rdd4").get)
    assert(program.transformations(2).asInstanceOf[SparkRDDBinaryTransformation].outputDataset.isDefined)
    assert(program.transformations(2).asInstanceOf[SparkRDDBinaryTransformation].outputDataset.get == program.datasetByReferenceName("rdd5").get)

    assert(program.edges.size == 9)

    assert(program.edges(0).dataset == program.datasetByReferenceName("rdd1").get)
    assert(program.edges(0).transformation == program.transformations(0))
    assert(program.edges(0).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(1).dataset == program.datasetByReferenceName("rdd2").get)
    assert(program.edges(1).transformation == program.transformations(0))
    assert(program.edges(1).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(2).dataset == program.datasetByReferenceName("rdd3").get)
    assert(program.edges(2).transformation == program.transformations(0))
    assert(program.edges(2).direction == DirectionsEnum.TransformationToDataset)

    assert(program.edges(3).dataset == program.datasetByReferenceName("rdd1").get)
    assert(program.edges(3).transformation == program.transformations(1))
    assert(program.edges(3).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(4).dataset == program.datasetByReferenceName("rdd2").get)
    assert(program.edges(4).transformation == program.transformations(1))
    assert(program.edges(4).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(5).dataset == program.datasetByReferenceName("rdd4").get)
    assert(program.edges(5).transformation == program.transformations(1))
    assert(program.edges(5).direction == DirectionsEnum.TransformationToDataset)

    assert(program.edges(6).dataset == program.datasetByReferenceName("rdd3").get)
    assert(program.edges(6).transformation == program.transformations(2))
    assert(program.edges(6).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(7).dataset == program.datasetByReferenceName("rdd4").get)
    assert(program.edges(7).transformation == program.transformations(2))
    assert(program.edges(7).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(8).dataset == program.datasetByReferenceName("rdd5").get)
    assert(program.edges(8).transformation == program.transformations(2))
    assert(program.edges(8).direction == DirectionsEnum.TransformationToDataset)

  }

  test("Test Case 4 - Program with Key-Value RDDs and Key-Value unary and  binary transformations (Join Transformation)") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]) : RDD[(Long, (Double, String))] = {
          val rdd3 = rdd1.reduceByKey((x, y) => if(x > y) x else y)
          val rdd4 = rdd3.join(rdd2)
          rdd4
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.String))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    assert(program.datasets.size == 4)

    assert(program.isDatasetByReferenceNameDefined("rdd1"))
    assert(program.datasetByReferenceName("rdd1").get.reference.referenceType == ReferencesTypeEnum.Parameter)
    assert(program.datasetByReferenceName("rdd1").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double)))))
    assert(program.datasetByReferenceName("rdd1").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd1").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd2"))
    assert(program.datasetByReferenceName("rdd2").get.reference.referenceType == ReferencesTypeEnum.Parameter)
    assert(program.datasetByReferenceName("rdd2").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.String)))))
    assert(program.datasetByReferenceName("rdd2").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd2").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd3"))
    assert(program.datasetByReferenceName("rdd3").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd3").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double)))))
    assert(!program.datasetByReferenceName("rdd3").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd3").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd4"))
    assert(program.datasetByReferenceName("rdd4").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd4").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.String))))))
    assert(!program.datasetByReferenceName("rdd4").get.isInputDataset)
    assert(program.datasetByReferenceName("rdd4").get.isOutputDataset)

    assert(program.transformations.size == 2)

    assert(program.transformations(0).isInstanceOf[SparkRDDUnaryTransformation])
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].name == "reduceByKey")
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].params.size == 1)
    assert(!program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].isLoadTransformation)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].inputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].inputDataset.get == program.datasetByReferenceName("rdd1").get)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].outputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].outputDataset.get == program.datasetByReferenceName("rdd3").get)

    assert(program.transformations(1).isInstanceOf[SparkRDDBinaryTransformation])
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].name == "join")
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].params.size == 1)
    assert(!program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].isLoadTransformation)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].firstInputDataset.isDefined)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].firstInputDataset.get == program.datasetByReferenceName("rdd3").get)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].secondInputDataset.isDefined)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].secondInputDataset.get == program.datasetByReferenceName("rdd2").get)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].outputDataset.isDefined)
    assert(program.transformations(1).asInstanceOf[SparkRDDBinaryTransformation].outputDataset.get == program.datasetByReferenceName("rdd4").get)

    assert(program.edges.size == 5)

    assert(program.edges(0).dataset == program.datasetByReferenceName("rdd1").get)
    assert(program.edges(0).transformation == program.transformations(0))
    assert(program.edges(0).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(1).dataset == program.datasetByReferenceName("rdd3").get)
    assert(program.edges(1).transformation == program.transformations(0))
    assert(program.edges(1).direction == DirectionsEnum.TransformationToDataset)

    assert(program.edges(2).dataset == program.datasetByReferenceName("rdd3").get)
    assert(program.edges(2).transformation == program.transformations(1))
    assert(program.edges(2).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(3).dataset == program.datasetByReferenceName("rdd2").get)
    assert(program.edges(3).transformation == program.transformations(1))
    assert(program.edges(3).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(4).dataset == program.datasetByReferenceName("rdd4").get)
    assert(program.edges(4).transformation == program.transformations(1))
    assert(program.edges(4).direction == DirectionsEnum.TransformationToDataset)

  }

  test("Test Case 5 - Program with not supported RDD transformations") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.cache
          val rdd3 = rdd2.zip(rdd1)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ParameterReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Int), BaseType(BaseTypesEnum.Int))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    assert(program.datasets.size == 3)

    assert(program.isDatasetByReferenceNameDefined("rdd1"))
    assert(program.datasetByReferenceName("rdd1").get.reference.referenceType == ReferencesTypeEnum.Parameter)
    assert(program.datasetByReferenceName("rdd1").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(program.datasetByReferenceName("rdd1").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd1").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd2"))
    assert(program.datasetByReferenceName("rdd2").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd2").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(!program.datasetByReferenceName("rdd2").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd2").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd3"))
    assert(program.datasetByReferenceName("rdd3").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd3").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Int), BaseType(BaseTypesEnum.Int)))))
    assert(!program.datasetByReferenceName("rdd3").get.isInputDataset)
    assert(program.datasetByReferenceName("rdd3").get.isOutputDataset)

    assert(program.transformations.size == 2)

    assert(program.transformations(0).isInstanceOf[SparkRDDOperation])
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].name == "cache")
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].params.isEmpty)
    assert(!program.transformations(0).asInstanceOf[SparkRDDOperation].isLoadTransformation)
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].incomingDatasets.size == 1)
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].incomingDatasets(0) == program.datasetByReferenceName("rdd1").get)
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].outgoingDatasets.size == 1)
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].outgoingDatasets(0) == program.datasetByReferenceName("rdd2").get)

    assert(program.transformations(1).isInstanceOf[SparkRDDOperation])
    assert(program.transformations(1).asInstanceOf[SparkRDDOperation].name == "zip")
    assert(program.transformations(1).asInstanceOf[SparkRDDOperation].params.size == 1)
    // In not supported transformations the parameters are treated generically
    // So rdd1 that is passed as input to zip is not treated the same way as it would be in a supported binary transformation
    assert(program.transformations(1).asInstanceOf[SparkRDDOperation].params.head.isEqual(q"rdd1"))
    assert(!program.transformations(1).asInstanceOf[SparkRDDOperation].isLoadTransformation)
    assert(program.transformations(1).asInstanceOf[SparkRDDOperation].incomingDatasets.size == 1)
    assert(program.transformations(1).asInstanceOf[SparkRDDOperation].incomingDatasets(0) == program.datasetByReferenceName("rdd2").get)
    assert(program.transformations(1).asInstanceOf[SparkRDDOperation].outgoingDatasets.size == 1)
    assert(program.transformations(1).asInstanceOf[SparkRDDOperation].outgoingDatasets(0) == program.datasetByReferenceName("rdd3").get)

    assert(program.edges.size == 4)

    assert(program.edges(0).dataset == program.datasetByReferenceName("rdd1").get)
    assert(program.edges(0).transformation == program.transformations(0))
    assert(program.edges(0).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(1).dataset == program.datasetByReferenceName("rdd2").get)
    assert(program.edges(1).transformation == program.transformations(0))
    assert(program.edges(1).direction == DirectionsEnum.TransformationToDataset)

    // Since parameters are handled generically in not supported transformations
    // The zip transformation is not recognized as a binary transformation
    // Therefore, a second edge is not created to link zip with rdd1
    assert(program.edges(2).dataset == program.datasetByReferenceName("rdd2").get)
    assert(program.edges(2).transformation == program.transformations(1))
    assert(program.edges(2).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(3).dataset == program.datasetByReferenceName("rdd3").get)
    assert(program.edges(3).transformation == program.transformations(1))
    assert(program.edges(3).direction == DirectionsEnum.TransformationToDataset)

  }

  test("Test Case 6 - Program with not supported RDD transformation (Action)") {

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

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    assert(program.datasets.size == 1)

    assert(program.isDatasetByReferenceNameDefined("rdd1"))
    assert(program.datasetByReferenceName("rdd1").get.reference.referenceType == ReferencesTypeEnum.Parameter)
    assert(program.datasetByReferenceName("rdd1").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(program.datasetByReferenceName("rdd1").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd1").get.isOutputDataset)

    assert(program.transformations.size == 1)

    assert(program.transformations(0).isInstanceOf[SparkRDDOperation])
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].name == "reduce")
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].params.size == 1)
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].params.head.isEqual(q"(a, b) => a + b"))
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].isLoadTransformation) // Should be true
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].incomingDatasets.size == 1)
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].incomingDatasets(0) == program.datasetByReferenceName("rdd1").get)
    assert(program.transformations(0).asInstanceOf[SparkRDDOperation].outgoingDatasets.size == 0) // Should be 0

    assert(program.edges.size == 1)

    assert(program.edges(0).dataset == program.datasetByReferenceName("rdd1").get)
    assert(program.edges(0).transformation == program.transformations(0))
    assert(program.edges(0).direction == DirectionsEnum.DatasetToTransformation)

  }

  test("Test Case 7 - Program with the input RDD being created inside the program rather than being passed as parameter and an unary transformation") {

    val tree: Tree = q"""
      import org.apache.spark.SparkContext
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(sc: SparkContext, input: List[Int]) = {
          val rdd1 = sc.parallelize(input)
          val rdd2 = rdd1.map(a => a * 2)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("sc" -> ParameterReference("sc", ParameterizedType("org/apache/spark/SparkContext#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("input" -> ParameterReference("input", ParameterizedType("List", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd1" -> ValReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    assert(program.datasets.size == 2)

    assert(program.isDatasetByReferenceNameDefined("rdd1"))
    assert(program.datasetByReferenceName("rdd1").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd1").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(program.datasetByReferenceName("rdd1").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd1").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd2"))
    assert(program.datasetByReferenceName("rdd2").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd2").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(!program.datasetByReferenceName("rdd2").get.isInputDataset)
    assert(program.datasetByReferenceName("rdd2").get.isOutputDataset)

    assert(program.transformations.size == 1)

    assert(program.transformations(0).isInstanceOf[SparkRDDUnaryTransformation])
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].name == "map")
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].params.size == 1)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].params.head.isEqual(q"a => a * 2"))
    assert(!program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].isLoadTransformation)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].inputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].inputDataset.get == program.datasetByReferenceName("rdd1").get)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].outputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDUnaryTransformation].outputDataset.get == program.datasetByReferenceName("rdd2").get)

    assert(program.edges.size == 2)

    assert(program.edges(0).dataset == program.datasetByReferenceName("rdd1").get)
    assert(program.edges(0).transformation == program.transformations(0))
    assert(program.edges(0).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(1).dataset == program.datasetByReferenceName("rdd2").get)
    assert(program.edges(1).transformation == program.transformations(0))
    assert(program.edges(1).direction == DirectionsEnum.TransformationToDataset)

  }
  
  test("Test Case 8 - Program with the input RDDs being created inside the program rather than being passed as parameter and a binary transformation") {

    val tree: Tree = q"""
      import org.apache.spark.SparkContext
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(sc: SparkContext, input1: List[Int], input2: List[Int]) = {
          val rdd1 = sc.parallelize(inpu1)
          val rdd2 = sc.parallelize(input2)
          val rdd3 = rdd1.union(rdd2)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("sc" -> ParameterReference("sc", ParameterizedType("org/apache/spark/SparkContext#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("input1" -> ParameterReference("input1", ParameterizedType("List", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("input2" -> ParameterReference("input2", ParameterizedType("List", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd1" -> ValReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    assert(program.datasets.size == 3)

    assert(program.isDatasetByReferenceNameDefined("rdd1"))
    assert(program.datasetByReferenceName("rdd1").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd1").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(program.datasetByReferenceName("rdd1").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd1").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd2"))
    assert(program.datasetByReferenceName("rdd2").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd2").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(program.datasetByReferenceName("rdd2").get.isInputDataset)
    assert(!program.datasetByReferenceName("rdd2").get.isOutputDataset)

    assert(program.isDatasetByReferenceNameDefined("rdd3"))
    assert(program.datasetByReferenceName("rdd3").get.reference.referenceType == ReferencesTypeEnum.Val)
    assert(program.datasetByReferenceName("rdd3").get.datasetType == ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int))))
    assert(!program.datasetByReferenceName("rdd3").get.isInputDataset)
    assert(program.datasetByReferenceName("rdd3").get.isOutputDataset)

    assert(program.transformations.size == 1)

    assert(program.transformations(0).isInstanceOf[SparkRDDBinaryTransformation])
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].name == "union")
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].params.size == 1)
    assert(!program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].isLoadTransformation)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].firstInputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].firstInputDataset.get == program.datasetByReferenceName("rdd1").get)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].secondInputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].secondInputDataset.get == program.datasetByReferenceName("rdd2").get)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].outputDataset.isDefined)
    assert(program.transformations(0).asInstanceOf[SparkRDDBinaryTransformation].outputDataset.get == program.datasetByReferenceName("rdd3").get)

    assert(program.edges.size == 3)

    assert(program.edges(0).dataset == program.datasetByReferenceName("rdd1").get)
    assert(program.edges(0).transformation == program.transformations(0))
    assert(program.edges(0).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(1).dataset == program.datasetByReferenceName("rdd2").get)
    assert(program.edges(1).transformation == program.transformations(0))
    assert(program.edges(1).direction == DirectionsEnum.DatasetToTransformation)

    assert(program.edges(2).dataset == program.datasetByReferenceName("rdd3").get)
    assert(program.edges(2).transformation == program.transformations(0))
    assert(program.edges(2).direction == DirectionsEnum.TransformationToDataset)

  }

}