package br.ufrn.dimap.forall.transmut.spark.analyzer

import br.ufrn.dimap.forall.transmut.analyzer.ProgramBuilder
import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.spark.model._
import br.ufrn.dimap.forall.util.LongIdGenerator
import scala.meta._
import scala.meta.Tree

object SparkRDDProgramBuilder extends ProgramBuilder {

  val datasetType = "org/apache/spark/rdd/RDD#"

  val suportedUnaryTransformations = Set("map", "flatMap", "filter", "distinct", "sortBy", "aggregateByKey", "groupByKey", "reduceByKey", "sortByKey")

  val suportedBinaryTransformations = Set("union", "subtract", "intersection", "subtractByKey", "join", "leftOuterJoin", "rightOuterJoin", "fullOuterJoin")

  def buildProgramSourceFromProgramNames(programNames: List[String], tree: Tree, refenceTypes: Map[String, Reference]): SparkRDDProgramSource = {

    val programSourceIds = LongIdGenerator.generator
    val programIds = LongIdGenerator.generator
    val datasetIds = LongIdGenerator.generator
    val transformationIds = LongIdGenerator.generator
    val edgesIds = LongIdGenerator.generator

    val programSource = SparkRDDProgramSource(programSourceIds.getId, tree)

    for (name <- programNames) {
      programSource.tree.traverse {

        // case q"..$mods def $methodName[..$tparams](...$paramss): $decltpe = $body" if methodName.value == name
        case d: Defn.Def if d.name.value == name => {
          val programTree = d
          val mods = d.mods
          val tparams = d.tparams
          val paramss = d.paramss
          val decltpe = d.decltpe
          val body = d.body
          //(mods, methodName, tparams, paramss, decltpe, body)

          val program = SparkRDDProgram(programIds.getId, name, programTree)

          // Input Datasets from Parameters
          paramss.foreach { p =>
            p.foreach { param =>
              // Parameter Datasets are defined only for parameters defined in referenceTypes
              val referenceName = refenceTypes.get(param.name.value)
              if (referenceName.isDefined) {
                val datasetId = datasetIds.getId
                val datasetName = param.name.value
                val originalReference = refenceTypes.get(datasetName).get
                val datasetReference = ParameterReference(originalReference.name, originalReference.valueType)
                // Observar aqui paramss.asInstanceOf[Tree]
                val datasetSource = programTree
                val dataset = SparkRDD(datasetId, datasetReference, datasetSource)
                program.addDataset(dataset)
              }
            }
          }

          body.traverse {
            // case q"..$mods val $valName: $tpeopt = $expr" => {
            case dv: Defn.Val => {
              val mods = dv.mods
              val valName = dv.pats.head
              val tpeopt = dv.decltpe
              val expr = dv.rhs

              val treeElement = dv

              var outputDatasetReferenceName: Option[String] = valName match {
                case Pat.Var(Term.Name(n)) => Some(n)
                case _                     => None
              }
              var firstInputDatasetReferenceName: Option[String] = None
              var secondInputDatasetReferenceName: Option[String] = None
              var transformationName: Option[String] = None
              var transformationParams: List[Tree] = List()
              expr match {
                case q"$dset.$transf(..$pars)" =>
                  {
                    firstInputDatasetReferenceName = dset match {
                      case Term.Name(n) => Some(n)
                      case _            => None
                    }
                    transformationName = transf match {
                      case Term.Name(n) => Some(n)
                      case _            => None
                    }
                    if (transformationName.isDefined && suportedBinaryTransformations.contains(transformationName.get)) {
                      secondInputDatasetReferenceName = pars.head match {
                        case Term.Name(n) => Some(n)
                        case _            => None
                      }
                    }
                    transformationParams = pars
                  }

                  if (firstInputDatasetReferenceName.isDefined && transformationName.isDefined && outputDatasetReferenceName.isDefined) {

                    val firstInputDataset: Dataset = program.datasetByReferenceName(firstInputDatasetReferenceName.get).getOrElse {
                      val referenceName = refenceTypes.get(firstInputDatasetReferenceName.get)
                      if (referenceName.isDefined) {
                        val datasetId = datasetIds.getId
                        val datasetName = firstInputDatasetReferenceName.get
                        val originalReference = refenceTypes.get(datasetName).get
                        val datasetReference = ValReference(originalReference.name, originalReference.valueType)
                        val datasetSource = programTree
                        val dataset = SparkRDD(datasetId, datasetReference, datasetSource)
                        dataset
                      } else null
                    }

                    val outputDataset: Dataset = program.datasetByReferenceName(outputDatasetReferenceName.get).getOrElse {
                      val referenceName = refenceTypes.get(outputDatasetReferenceName.get)
                      if (referenceName.isDefined) {
                        val datasetId = datasetIds.getId
                        val datasetName = outputDatasetReferenceName.get
                        val originalReference = refenceTypes.get(datasetName).get
                        val datasetReference = ValReference(originalReference.name, originalReference.valueType)
                        val datasetSource = programTree
                        val dataset = SparkRDD(datasetId, datasetReference, datasetSource)
                        dataset
                      } else null
                    }

                    val transformation: SparkRDDTransformation = if (suportedUnaryTransformations.contains(transformationName.get)) {
                      SparkRDDUnaryTransformation(transformationIds.getId, transformationName.get, transformationParams, treeElement)
                    } else if (suportedBinaryTransformations.contains(transformationName.get)) {
                      SparkRDDBinaryTransformation(transformationIds.getId, transformationName.get, treeElement)
                    } else null

                    val secondInputDataset: Dataset = if (secondInputDatasetReferenceName.isDefined && suportedBinaryTransformations.contains(transformationName.get)) {
                      program.datasetByReferenceName(secondInputDatasetReferenceName.get).getOrElse {
                        val referenceName = refenceTypes.get(secondInputDatasetReferenceName.get)
                        if (referenceName.isDefined) {
                          val datasetId = datasetIds.getId
                          val datasetName = secondInputDatasetReferenceName.get
                          val originalReference = refenceTypes.get(datasetName).get
                          val datasetReference = ValReference(originalReference.name, originalReference.valueType)
                          val datasetSource = paramss.asInstanceOf[Tree]
                          val dataset = SparkRDD(datasetId, datasetReference, datasetSource)
                          dataset
                        } else null
                      }
                    } else null

                    if (firstInputDataset != null && transformation != null && outputDataset != null && transformation.isInstanceOf[SparkRDDUnaryTransformation]) {
                      val incomeEdge = SparkRDDEdge(edgesIds.getId, firstInputDataset.asInstanceOf[SparkRDD], transformation, DirectionsEnum.DatasetToTransformation)
                      val outcomeEdge = SparkRDDEdge(edgesIds.getId, outputDataset.asInstanceOf[SparkRDD], transformation, DirectionsEnum.TransformationToDataset)
                      // Update edges references
                      firstInputDataset.asInstanceOf[SparkRDD].addEdge(incomeEdge)
                      outputDataset.asInstanceOf[SparkRDD].addEdge(outcomeEdge)
                      transformation.asInstanceOf[SparkRDDUnaryTransformation].addInputEdge(incomeEdge)
                      transformation.asInstanceOf[SparkRDDUnaryTransformation].addOutputEdge(outcomeEdge)
                      // Only add a dataset if it is not already defined
                      if (!program.isDatasetByReferenceNameDefined(firstInputDataset.name))
                        program.addDataset(firstInputDataset.asInstanceOf[SparkRDD])
                      if (!program.isDatasetByReferenceNameDefined(outputDataset.name))
                        program.addDataset(outputDataset.asInstanceOf[SparkRDD])
                      program.addTransformation(transformation)
                      program.addEdge(incomeEdge)
                      program.addEdge(outcomeEdge)
                    } else if (firstInputDataset != null && transformation != null && outputDataset != null && transformation.isInstanceOf[SparkRDDBinaryTransformation] && secondInputDataset != null) {
                      val firstIncomeEdge = SparkRDDEdge(edgesIds.getId, firstInputDataset.asInstanceOf[SparkRDD], transformation, DirectionsEnum.DatasetToTransformation)
                      val secondIncomeEdge = SparkRDDEdge(edgesIds.getId, secondInputDataset.asInstanceOf[SparkRDD], transformation, DirectionsEnum.DatasetToTransformation)
                      val outcomeEdge = SparkRDDEdge(edgesIds.getId, outputDataset.asInstanceOf[SparkRDD], transformation, DirectionsEnum.TransformationToDataset)
                      // Update edges references
                      firstInputDataset.asInstanceOf[SparkRDD].addEdge(firstIncomeEdge)
                      secondInputDataset.asInstanceOf[SparkRDD].addEdge(secondIncomeEdge)
                      outputDataset.asInstanceOf[SparkRDD].addEdge(outcomeEdge)
                      transformation.asInstanceOf[SparkRDDBinaryTransformation].addFirstInputEdge(firstIncomeEdge)
                      transformation.asInstanceOf[SparkRDDBinaryTransformation].addSecondInputEdge(secondIncomeEdge)
                      transformation.asInstanceOf[SparkRDDBinaryTransformation].addOutputEdge(outcomeEdge)

                      // Only add a dataset if it is not already defined
                      if (!program.isDatasetByReferenceNameDefined(firstInputDataset.name))
                        program.addDataset(firstInputDataset.asInstanceOf[SparkRDD])
                      if (!program.isDatasetByReferenceNameDefined(secondInputDataset.name))
                        program.addDataset(secondInputDataset.asInstanceOf[SparkRDD])
                      if (!program.isDatasetByReferenceNameDefined(outputDataset.name))
                        program.addDataset(outputDataset.asInstanceOf[SparkRDD])
                      program.addTransformation(transformation)
                      program.addEdge(firstIncomeEdge)
                      program.addEdge(secondIncomeEdge)
                      program.addEdge(outcomeEdge)
                    }

                  }

              }
            }

          }
          programSource.addProgram(program)
        }

      }

    }
    programSource
  }
}