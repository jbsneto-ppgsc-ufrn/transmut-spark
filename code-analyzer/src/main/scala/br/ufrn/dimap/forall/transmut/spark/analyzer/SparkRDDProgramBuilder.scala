package br.ufrn.dimap.forall.transmut.spark.analyzer

import scala.meta._
import scala.meta.Tree

import br.ufrn.dimap.forall.transmut.analyzer.ProgramBuilder
import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.spark.model._
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import java.nio.file.Files
import java.nio.file.Path
import br.ufrn.dimap.forall.transmut.util.IOFiles
import br.ufrn.dimap.forall.transmut.analyzer.CodeParser
import br.ufrn.dimap.forall.transmut.analyzer.TypesAnalyzer
import br.ufrn.dimap.forall.transmut.exception.ProgramBuildException

object SparkRDDProgramBuilder extends ProgramBuilder {

  val datasetType = "org/apache/spark/rdd/RDD#"

  val supportedUnaryTransformations = Set("map", "flatMap", "filter", "distinct", "sortBy", "aggregateByKey", "reduceByKey", "sortByKey", "combineByKey")

  val supportedBinaryTransformations = Set("union", "subtract", "intersection", "join", "leftOuterJoin", "rightOuterJoin", "fullOuterJoin")

  // Class that aggregates all ID generators
  private class IdsGeneratorAggregator {

    val programSourceIds = LongIdGenerator.generator
    val programIds = LongIdGenerator.generator
    val datasetIds = LongIdGenerator.generator
    val transformationIds = LongIdGenerator.generator
    val edgesIds = LongIdGenerator.generator

    def programSourceId = programSourceIds.getId
    def programId = programIds.getId
    def datasetId = datasetIds.getId
    def transformationId = transformationIds.getId
    def edgesId = edgesIds.getId
  }

  override def buildProgramSources(sources: List[String], programs: List[String], srcDir: Path, semanticdbDir: Path): List[SparkRDDProgramSource] = {
    val filesInSrcDir = IOFiles.getListOfFiles(srcDir.toFile)
    val filesInSemanticdbDir = IOFiles.getListOfFiles(semanticdbDir.toFile)
    val sourceSemanticdbTuplePaths = scala.collection.mutable.ListBuffer.empty[(Path, Path)] // List of (source, semanticdb)
    for (source <- sources) {
      val sourcePath = filesInSrcDir.filter(f => f.isFile() && f.getAbsolutePath.contains(source)).headOption
      if (sourcePath.isEmpty)
        throw new ProgramBuildException("Source " + source + " does not exists in " + srcDir.toFile().getAbsolutePath)
      val semanticdbPath = filesInSemanticdbDir.filter(f => f.isFile() && f.getAbsolutePath.contains(source) && f.getAbsolutePath.contains("semanticdb")).headOption
      if (semanticdbPath.isEmpty)
        throw new ProgramBuildException("Semanticdb document for " + source + " does not exists in " + semanticdbDir.toFile().getAbsolutePath)
      sourceSemanticdbTuplePaths += ((sourcePath.get.toPath(), semanticdbPath.get.toPath()))
    }
    val programSources = scala.collection.mutable.ListBuffer.empty[SparkRDDProgramSource]
    val ids = new IdsGeneratorAggregator
    for (sourceSemanticdb <- sourceSemanticdbTuplePaths) {
      val sourcePath = sourceSemanticdb._1
      val semanticdbPath = sourceSemanticdb._2
      val tree = CodeParser.getTreeFromPath(sourcePath)
      val referenceTypes = TypesAnalyzer.getReferenceMapFromPath(semanticdbPath)
      val programSource = buildProgramSourceFromProgramNames(programs, tree, referenceTypes, sourcePath, ids)
      programSources += programSource
    }
    // Verify if the programs in programs list were created
    val programsNames = programSources.flatMap(ps => ps.programs.map(p => p.name))
    for (program <- programs) {
      if (!programsNames.contains(program))
        throw new ProgramBuildException("Program " + program + " does not exists in the program sources")
    }
    programSources.toList
  }

  // For tests where the source file is not necessary, it creates a temp file.
  def buildProgramSourceFromProgramNames(programNames: List[String], tree: Tree, refenceTypes: Map[String, Reference]): SparkRDDProgramSource = buildProgramSourceFromProgramNames(programNames, tree, refenceTypes, Files.createTempFile("TempProgramSource", ".scala"), new IdsGeneratorAggregator)

  def buildProgramSourceFromProgramNames(programNames: List[String], tree: Tree, refenceTypes: Map[String, Reference], source: Path, ids: IdsGeneratorAggregator): SparkRDDProgramSource = {
    val programSource = SparkRDDProgramSource(ids.programSourceId, tree, source)

    for (name <- programNames) {
      programSource.tree.traverse {
        case d: Defn.Def if d.name.value == name => {
          val program = buildProgramFromTree(programSource, name, d, refenceTypes, ids)
          if (program.isDefined) {
            programSource.addProgram(program.get.asInstanceOf[SparkRDDProgram])
          }
        }
      }

    }
    programSource
  }

  private def buildProgramFromTree(programSource: SparkRDDProgramSource, programName: String, programTree: Tree, refenceTypes: Map[String, Reference], ids: IdsGeneratorAggregator): Option[Program] = {

    var programReturn: Option[Program] = None

    programTree.traverse {

      case q"..$mods def $methodName[..$tparams](...$paramss): $decltpe = $body" if methodName.value == programName => {

        val program = SparkRDDProgram(ids.programId, programName, programTree, programSource)

        // Input Datasets from Parameters
        paramss.foreach { p =>
          p.foreach { param =>
            // Parameter Datasets are defined only for parameters defined in referenceTypes
            val referenceName = refenceTypes.get(param.name.value)
            if (referenceName.isDefined && referenceName.get.valueType.name == datasetType) {
              val datasetId = ids.datasetId
              val datasetName = param.name.value
              val originalReference = refenceTypes.get(datasetName).get
              val datasetReference = ParameterReference(originalReference.name, originalReference.valueType)
              val datasetSource = programTree
              val dataset = SparkRDD(datasetId, program, datasetReference, datasetSource)
              program.addDataset(dataset)
            }
          }
        }

        body.traverse {
          // case q"..$mods val $valName: $tpeopt = $expr" => {
          // case dv: Defn.Val
          // An expression should be defined as a val or var statement
          case dv if dv.isInstanceOf[Defn.Val] || dv.isInstanceOf[Defn.Var] => {
            var mods: List[Mod] = Nil
            var pats: List[Pat] = Nil
            var valName: Option[Pat] = None
            var tpeopt: Option[scala.meta.Type] = None
            var expr: Option[Term] = None
            var isValReference = true

            if (dv.isInstanceOf[Defn.Val]) {
              val valTree = dv.asInstanceOf[Defn.Val]
              mods = valTree.mods
              pats = valTree.pats
              valName = Some(pats.head)
              tpeopt = valTree.decltpe
              expr = Some(valTree.rhs)
              isValReference = true
            } else {
              val varTree = dv.asInstanceOf[Defn.Var]
              mods = varTree.mods
              pats = varTree.pats
              valName = Some(pats.head)
              tpeopt = varTree.decltpe
              expr = varTree.rhs
              isValReference = false
            }

            if (valName.isDefined && expr.isDefined) {

              val treeElement = dv

              var outputDatasetReferenceName: Option[String] = valName.get match {
                case Pat.Var(Term.Name(n)) => Some(n)
                case _                     => None
              }
              var firstInputDatasetReferenceName: Option[String] = None
              var secondInputDatasetReferenceName: Option[String] = None
              var transformationName: Option[String] = None
              var transformationParams: List[Tree] = List()
              expr.get match {
                case q"$dset.$transf(..$pars)" => {
                  firstInputDatasetReferenceName = dset match {
                    case Term.Name(n) => Some(n)
                    case _            => None
                  }
                  transformationName = transf match {
                    case Term.Name(n) => Some(n)
                    case _            => None
                  }
                  if (transformationName.isDefined && supportedBinaryTransformations.contains(transformationName.get)) {
                    secondInputDatasetReferenceName = pars.head match {
                      case Term.Name(n) => Some(n)
                      case _            => None
                    }
                  }
                  transformationParams = pars
                }
                case q"$dset.$transf" => {
                  firstInputDatasetReferenceName = dset match {
                    case Term.Name(n) => Some(n)
                    case _            => None
                  }
                  transformationName = transf match {
                    case Term.Name(n) => Some(n)
                    case _            => None
                  }
                }
              }

              if (firstInputDatasetReferenceName.isDefined && transformationName.isDefined && outputDatasetReferenceName.isDefined) {

                val firstInputDataset: Option[SparkRDD] = if (program.isDatasetByReferenceNameDefined(firstInputDatasetReferenceName.get)) {
                  // First, it is checked if the dataset has already been defined, if so it is taken, otherwise a new dataset is created
                  Some(program.datasetByReferenceName(firstInputDatasetReferenceName.get).get.asInstanceOf[SparkRDD])
                } else {
                  val referenceName = refenceTypes.get(firstInputDatasetReferenceName.get)
                  // Only creates a new dataset if the reference is an RDD
                  if (referenceName.isDefined && referenceName.get.valueType.name == datasetType) {
                    val datasetId = ids.datasetId
                    val datasetName = firstInputDatasetReferenceName.get
                    val originalReference = refenceTypes.get(datasetName).get
                    // The types analyzer only generates "val" references, here I adjust if it is "var" reference
                    val datasetReference = if (isValReference) ValReference(originalReference.name, originalReference.valueType) else VarReference(originalReference.name, originalReference.valueType)
                    val datasetSource = treeElement
                    val dataset = SparkRDD(datasetId, program, datasetReference, datasetSource)
                    Some(dataset)
                  } else None
                }

                val outputDataset: Option[SparkRDD] = if (program.isDatasetByReferenceNameDefined(outputDatasetReferenceName.get)) {
                  // First, it is checked if the dataset has already been defined, if so it is taken, otherwise a new dataset is created
                  Some(program.datasetByReferenceName(outputDatasetReferenceName.get).get.asInstanceOf[SparkRDD])
                } else {
                  val referenceName = refenceTypes.get(outputDatasetReferenceName.get)
                  // Only creates a new dataset if the reference is an RDD
                  if (referenceName.isDefined && referenceName.get.valueType.name == datasetType) {
                    val datasetId = ids.datasetId
                    val datasetName = outputDatasetReferenceName.get
                    val originalReference = refenceTypes.get(datasetName).get
                    // The types analyzer only generates "val" references, here I adjust if it is "var" reference
                    val datasetReference = if (isValReference) ValReference(originalReference.name, originalReference.valueType) else VarReference(originalReference.name, originalReference.valueType)
                    val datasetSource = treeElement
                    val dataset = SparkRDD(datasetId, program, datasetReference, datasetSource)
                    Some(dataset)
                  } else None
                }

                val transformation: Option[SparkRDDTransformation] = if (supportedUnaryTransformations.contains(transformationName.get)) {
                  Some(SparkRDDUnaryTransformation(ids.transformationId, program, transformationName.get, transformationParams, treeElement))
                } else if (supportedBinaryTransformations.contains(transformationName.get)) {
                  Some(SparkRDDBinaryTransformation(ids.transformationId, program, transformationName.get, transformationParams, treeElement))
                } else if (firstInputDataset.isDefined) {
                  // For other RDD operations that are not supported transformations, including actions (the input dataset should be an RDD)
                  Some(SparkRDDOperation(ids.transformationId, program, transformationName.get, transformationParams, treeElement))
                } else None

                val secondInputDataset: Option[SparkRDD] = if (secondInputDatasetReferenceName.isDefined && supportedBinaryTransformations.contains(transformationName.get)) {
                  // First, it is checked if the dataset has already been defined, if so it is taken, otherwise a new dataset is created
                  if (program.isDatasetByReferenceNameDefined(secondInputDatasetReferenceName.get)) {
                    Some(program.datasetByReferenceName(secondInputDatasetReferenceName.get).get.asInstanceOf[SparkRDD])
                  } else {
                    val referenceName = refenceTypes.get(secondInputDatasetReferenceName.get)
                    // Only creates a new dataset if the reference is an RDD
                    if (referenceName.isDefined && referenceName.get.valueType.name == datasetType) {
                      val datasetId = ids.datasetId
                      val datasetName = secondInputDatasetReferenceName.get
                      val originalReference = refenceTypes.get(datasetName).get
                      // The types analyzer only generates "val" references, here I adjust if it is "var" reference
                      val datasetReference = if (isValReference) ValReference(originalReference.name, originalReference.valueType) else VarReference(originalReference.name, originalReference.valueType)
                      val datasetSource = treeElement
                      val dataset = SparkRDD(datasetId, program, datasetReference, datasetSource)
                      Some(dataset)
                    } else None
                  }
                } else None

                if (firstInputDataset.isDefined && transformation.isDefined && outputDataset.isDefined && transformation.get.isInstanceOf[SparkRDDUnaryTransformation]) {
                  val incomeEdge = SparkRDDEdge(ids.edgesId, firstInputDataset.get, transformation.get, DirectionsEnum.DatasetToTransformation)
                  val outcomeEdge = SparkRDDEdge(ids.edgesId, outputDataset.get, transformation.get, DirectionsEnum.TransformationToDataset)
                  // Update edges references
                  firstInputDataset.get.addEdge(incomeEdge)
                  outputDataset.get.addEdge(outcomeEdge)
                  transformation.get.asInstanceOf[SparkRDDUnaryTransformation].addInputEdge(incomeEdge)
                  transformation.get.asInstanceOf[SparkRDDUnaryTransformation].addOutputEdge(outcomeEdge)
                  // Only add a dataset if it is not already defined
                  if (!program.isDatasetByReferenceNameDefined(firstInputDataset.get.name))
                    program.addDataset(firstInputDataset.get)
                  if (!program.isDatasetByReferenceNameDefined(outputDataset.get.name))
                    program.addDataset(outputDataset.get)
                  program.addTransformation(transformation.get)
                  program.addEdge(incomeEdge)
                  program.addEdge(outcomeEdge)
                } else if (firstInputDataset.isDefined && transformation.isDefined && outputDataset.isDefined && transformation.get.isInstanceOf[SparkRDDBinaryTransformation] && secondInputDataset.isDefined) {
                  val firstIncomeEdge = SparkRDDEdge(ids.edgesId, firstInputDataset.get, transformation.get, DirectionsEnum.DatasetToTransformation)
                  val secondIncomeEdge = SparkRDDEdge(ids.edgesId, secondInputDataset.get, transformation.get, DirectionsEnum.DatasetToTransformation)
                  val outcomeEdge = SparkRDDEdge(ids.edgesId, outputDataset.get, transformation.get, DirectionsEnum.TransformationToDataset)
                  // Update edges references
                  firstInputDataset.get.addEdge(firstIncomeEdge)
                  secondInputDataset.get.addEdge(secondIncomeEdge)
                  outputDataset.get.addEdge(outcomeEdge)
                  transformation.get.asInstanceOf[SparkRDDBinaryTransformation].addFirstInputEdge(firstIncomeEdge)
                  transformation.get.asInstanceOf[SparkRDDBinaryTransformation].addSecondInputEdge(secondIncomeEdge)
                  transformation.get.asInstanceOf[SparkRDDBinaryTransformation].addOutputEdge(outcomeEdge)

                  // Only add a dataset if it is not already defined
                  if (!program.isDatasetByReferenceNameDefined(firstInputDataset.get.name))
                    program.addDataset(firstInputDataset.get)
                  if (!program.isDatasetByReferenceNameDefined(secondInputDataset.get.name))
                    program.addDataset(secondInputDataset.get)
                  if (!program.isDatasetByReferenceNameDefined(outputDataset.get.name))
                    program.addDataset(outputDataset.get)
                  program.addTransformation(transformation.get)
                  program.addEdge(firstIncomeEdge)
                  program.addEdge(secondIncomeEdge)
                  program.addEdge(outcomeEdge)
                } else if (firstInputDataset.isDefined && transformation.isDefined && transformation.get.isInstanceOf[SparkRDDOperation]) {
                  val incomeEdge = SparkRDDEdge(ids.edgesId, firstInputDataset.get, transformation.get, DirectionsEnum.DatasetToTransformation)
                  // Update edges references
                  firstInputDataset.get.addEdge(incomeEdge)
                  transformation.get.addEdge(incomeEdge)
                  // Only add a dataset if it is not already defined
                  if (!program.isDatasetByReferenceNameDefined(firstInputDataset.get.name))
                    program.addDataset(firstInputDataset.get)
                  program.addTransformation(transformation.get)
                  program.addEdge(incomeEdge)

                  // if the output is an RDD, add it as a dataset to the program and update edges references, otherwise do not add it (in case of actions, for example)
                  if (outputDataset.isDefined) {
                    var outcomeEdge = SparkRDDEdge(ids.edgesId, outputDataset.get, transformation.get, DirectionsEnum.TransformationToDataset)
                    outputDataset.get.addEdge(outcomeEdge)
                    transformation.get.addEdge(outcomeEdge)
                    if (!program.isDatasetByReferenceNameDefined(outputDataset.get.name))
                      program.addDataset(outputDataset.get)
                    program.addEdge(outcomeEdge)
                  }
                }

              }

            } else {
              throw new ProgramBuildException("Expression not defined: " + dv.toString)
            }
          }
        }
        programReturn = Some(program)
      }
    }
    programReturn
  }
}