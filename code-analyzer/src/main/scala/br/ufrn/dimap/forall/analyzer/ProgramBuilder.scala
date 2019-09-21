package br.ufrn.dimap.forall.analyzer

import scala.meta._
import scala.meta.Tree

import br.ufrn.dimap.forall.model._
import br.ufrn.dimap.forall.model.Reference
import br.ufrn.dimap.forall.util.LongIdGenerator

object ProgramBuilder {

  def buildProgramSourceFromReferenceNames(programNames: List[String], tree: Tree, refenceTypes: Map[String, Reference]): ProgramSource = {

    val datasetType = "org/apache/spark/rdd/RDD#"

    val suportedUnaryTransformations = Set("map", "flatMap", "filter", "distinct", "sortBy", "aggregateByKey", "groupByKey", "reduceByKey", "sortByKey")
    val suportedBinaryTransformations = Set("union", "subtract", "intersection", "subtractByKey", "join", "leftOuterJoin", "rightOuterJoin", "fullOuterJoin")

    val programSourceIds = LongIdGenerator.generator
    val programIds = LongIdGenerator.generator
    val treeElementId = LongIdGenerator.generator
    val datasetIds = LongIdGenerator.generator
    val transformationIds = LongIdGenerator.generator
    val edgesIds = LongIdGenerator.generator

    val programSource = ProgramSource(programSourceIds.getId, tree)

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

          val program = Program(programIds.getId, name, programTree)

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
                val datasetSource = TreeElement(treeElementId.getId, programTree)
                val dataset = Dataset(datasetId, datasetReference, datasetSource)
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

              val treeElement = TreeElement(treeElementId.getId, dv)
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
                        val datasetSource = TreeElement(treeElementId.getId, programTree)
                        val dataset = Dataset(datasetId, datasetReference, datasetSource)
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
                        val datasetSource = TreeElement(treeElementId.getId, programTree)
                        val dataset = Dataset(datasetId, datasetReference, datasetSource)
                        dataset
                      } else null
                    }

                    val transformation: Transformation = if (suportedUnaryTransformations.contains(transformationName.get)) {
                      UnaryTransformation(transformationIds.getId, transformationName.get, transformationParams, treeElement)
                    } else if (suportedBinaryTransformations.contains(transformationName.get)) {
                      BinaryTransformation(transformationIds.getId, transformationName.get, treeElement)
                    } else null

                    val secondInputDataset: Dataset = if (secondInputDatasetReferenceName.isDefined && suportedBinaryTransformations.contains(transformationName.get)) {
                      program.datasetByReferenceName(secondInputDatasetReferenceName.get).getOrElse {
                        val referenceName = refenceTypes.get(secondInputDatasetReferenceName.get)
                        if (referenceName.isDefined) {
                          val datasetId = datasetIds.getId
                          val datasetName = secondInputDatasetReferenceName.get
                          val originalReference = refenceTypes.get(datasetName).get
                          val datasetReference = ValReference(originalReference.name, originalReference.valueType)
                          val datasetSource = TreeElement(treeElementId.getId, paramss.asInstanceOf[Tree])
                          val dataset = Dataset(datasetId, datasetReference, datasetSource)
                          dataset
                        } else null
                      }
                    } else null

                    if (firstInputDataset != null && transformation != null && outputDataset != null && transformation.isInstanceOf[UnaryTransformation]) {
                      val incomeEdge = Edge(edgesIds.getId, firstInputDataset, transformation, DirectionsEnum.DatasetToTransformation)
                      val outcomeEdge = Edge(edgesIds.getId, outputDataset, transformation, DirectionsEnum.TransformationToDataset)
                      // Update edges references
                      firstInputDataset.addEdge(incomeEdge)
                      outputDataset.addEdge(outcomeEdge)
                      transformation.asInstanceOf[UnaryTransformation].addInputEdge(incomeEdge)
                      transformation.asInstanceOf[UnaryTransformation].addOutputEdge(outcomeEdge)
                      // Add elements to the program
                      program.addTreeElement(treeElement)
                      // Only add a dataset if it is not already defined
                      if (!program.isDatasetByReferenceNameDefined(firstInputDataset.name))
                        program.addDataset(firstInputDataset)
                      if (!program.isDatasetByReferenceNameDefined(outputDataset.name))
                        program.addDataset(outputDataset)
                      program.addTransformation(transformation)
                      program.addEdge(incomeEdge)
                      program.addEdge(outcomeEdge)
                    } else if (firstInputDataset != null && transformation != null && outputDataset != null && transformation.isInstanceOf[BinaryTransformation] && secondInputDataset != null) {
                      val firstIncomeEdge = Edge(edgesIds.getId, firstInputDataset, transformation, DirectionsEnum.DatasetToTransformation)
                      val secondIncomeEdge = Edge(edgesIds.getId, secondInputDataset, transformation, DirectionsEnum.DatasetToTransformation)
                      val outcomeEdge = Edge(edgesIds.getId, outputDataset, transformation, DirectionsEnum.TransformationToDataset)
                      // Update edges references
                      firstInputDataset.addEdge(firstIncomeEdge)
                      secondInputDataset.addEdge(secondIncomeEdge)
                      outputDataset.addEdge(outcomeEdge)
                      transformation.asInstanceOf[BinaryTransformation].addFirstInputEdge(firstIncomeEdge)
                      transformation.asInstanceOf[BinaryTransformation].addSecondInputEdge(secondIncomeEdge)
                      transformation.asInstanceOf[BinaryTransformation].addOutputEdge(outcomeEdge)
                      // Add elements to the program
                      program.addTreeElement(treeElement)
                      // Only add a dataset if it is not already defined
                      if (!program.isDatasetByReferenceNameDefined(firstInputDataset.name))
                        program.addDataset(firstInputDataset)
                      if (!program.isDatasetByReferenceNameDefined(secondInputDataset.name))
                        program.addDataset(secondInputDataset)
                      if (!program.isDatasetByReferenceNameDefined(outputDataset.name))
                        program.addDataset(outputDataset)
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