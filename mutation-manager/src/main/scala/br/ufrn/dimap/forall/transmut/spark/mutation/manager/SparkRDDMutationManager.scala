package br.ufrn.dimap.forall.transmut.spark.mutation.manager

import scala.meta._
import scala.meta.contrib._

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.mutation.manager.MutationManager
import br.ufrn.dimap.forall.transmut.mutation.model._
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._

import br.ufrn.dimap.forall.transmut.spark.model._
import br.ufrn.dimap.forall.transmut.spark.mutation.operator._

import br.ufrn.dimap.forall.transmut.util.LongIdGenerator

object SparkRDDMutationManager extends MutationManager {

  def generateMutantsFromProgramSource(programSource: ProgramSource, mutationOperators: List[MutationOperatorsEnum], idGenerator: LongIdGenerator): List[MutantProgramSource] = {
    if (programSource.isInstanceOf[SparkRDDProgramSource]) {
      val sparkRDDProgramSource = programSource.asInstanceOf[SparkRDDProgramSource]
      val mutants = scala.collection.mutable.ListBuffer.empty[Mutant[Program]]
      for (program <- sparkRDDProgramSource.programs) {
        mutants ++= generateMutantsFromProgram(program, mutationOperators, idGenerator)
      }
      mutants.map(mutantProgram => generateMutantProgramSourceFromMutantProgram(sparkRDDProgramSource, mutantProgram.asInstanceOf[MutantProgram])).toList
    } else {
      Nil
    }
  }

  def generateMutantsFromProgram(program: Program, mutationOperators: List[MutationOperatorsEnum], idGenerator: LongIdGenerator): List[MutantProgram] = {
    if (program.isInstanceOf[SparkRDDProgram]) {
      val sparkRDDProgram = program.asInstanceOf[SparkRDDProgram]
      val transformations = sparkRDDProgram.transformations
      val mutants = scala.collection.mutable.ListBuffer.empty[Mutant[_]]
      for (operator <- mutationOperators) {
        operator match {
          case UTS => {
            mutants ++= SparkRDDUnaryTransformationSwap.generateMutants(transformations, idGenerator)
          }
          case BTS => {
            mutants ++= SparkRDDBinaryTransformationSwap.generateMutants(transformations, idGenerator)
          }
          case UTR => {
            mutants ++= SparkRDDUnaryTransformationReplacement.generateMutants(transformations, idGenerator)
          }
          case BTR => {
            mutants ++= SparkRDDBinaryTransformationReplacement.generateMutants(transformations, idGenerator)
          }
          case UTD => {
            mutants ++= SparkRDDUnaryTransformationDeletion.generateMutants(transformations, idGenerator)
          }
          case MTR => {
            transformations.foreach { transformation =>
              mutants ++= SparkRDDMappingTransformationReplacement.generateMutants(transformation, idGenerator)
            }
          }
          case FTD => {
            transformations.foreach { transformation =>
              mutants ++= SparkRDDFilterTransformationDeletion.generateMutants(transformation, idGenerator)
            }
          }
          case DTD => {
            transformations.foreach { transformation =>
              mutants ++= SparkRDDDistinctTransformationDeletion.generateMutants(transformation, idGenerator)
            }
          }
          case OTD => {
            transformations.foreach { transformation =>
              mutants ++= SparkRDDOrderTransformationDeletion.generateMutants(transformation, idGenerator)
            }
          }
          case STR => {
            transformations.foreach { transformation =>
              mutants ++= SparkRDDSetTransformationReplacement.generateMutants(transformation, idGenerator)
            }
          }
          case ATR => {
            transformations.foreach { transformation =>
              mutants ++= SparkRDDAggregationTransformationReplacement.generateMutants(transformation, idGenerator)
            }
          }
          case DTI => {
            transformations.foreach { transformation =>
              mutants ++= SparkRDDDistinctTransformationInsertion.generateMutants(transformation, idGenerator)
            }
          }
          case JTR => {
            transformations.foreach { transformation =>
              mutants ++= SparkRDDJoinTransformationReplacement.generateMutants(transformation, idGenerator)
            }
          }
        }
      }
      val mutantPrograms = mutants.map(mutantTransformation => {
        if (mutantTransformation.isInstanceOf[MutantTransformation]) {
          generateMutantProgramFromMutantTransformation(sparkRDDProgram, mutantTransformation.asInstanceOf[MutantTransformation])
        } else {
          generateMutantProgramFromMutantListTransformation(sparkRDDProgram, mutantTransformation.asInstanceOf[MutantListTransformation])
        }
      })
      mutantPrograms.toList
    } else {
      Nil
    }
  }

  private def generateMutantProgramFromMutantTransformation(originalProgram: SparkRDDProgram, mutantTransformation: MutantTransformation): MutantProgram = {
    val originalTransformation = mutantTransformation.original.asInstanceOf[SparkRDDTransformation]
    val mutatedTransformation = mutantTransformation.mutated.asInstanceOf[SparkRDDTransformation]
    val mutatedProgram = originalProgram.copy().asInstanceOf[SparkRDDProgram]
    mutatedProgram.removeTransformation(originalTransformation)
    mutatedProgram.addTransformation(mutatedTransformation)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case node if node.isEqual(originalTransformation.source) => mutatedTransformation.source
        case node => super.apply(node)
      }
    }
    mutatedProgram.tree = transformer(mutatedProgram.tree)
    MutantProgram(originalProgram, mutatedProgram, mutantTransformation)
  }

  private def generateMutantProgramFromMutantListTransformation(originalProgram: SparkRDDProgram, mutantListTransformation: MutantListTransformation): MutantProgram = {
    val mutatedProgram = originalProgram.copy().asInstanceOf[SparkRDDProgram]
    for (transformation <- mutantListTransformation.mutated) {
      val mutatedTransformation = transformation.asInstanceOf[SparkRDDTransformation]
      val originalTransformation = mutantListTransformation.original.filter(t => t.id == mutatedTransformation.id).head.asInstanceOf[SparkRDDTransformation]
      mutatedProgram.removeTransformation(originalTransformation)
      mutatedProgram.addTransformation(mutatedTransformation)
      val transformer = new Transformer {
        override def apply(tree: Tree): Tree = tree match {
          case node if node.isEqual(originalTransformation.source) => mutatedTransformation.source
          case node => super.apply(node)
        }
      }
      mutatedProgram.tree = transformer(mutatedProgram.tree)
    }
    MutantProgram(originalProgram, mutatedProgram, mutantListTransformation)
  }

  private def generateMutantProgramSourceFromMutantProgram(originalProgramSource: SparkRDDProgramSource, mutantProgram: MutantProgram): MutantProgramSource = {
    val originalProgram = mutantProgram.original.asInstanceOf[SparkRDDProgram]
    val mutatedProgram = mutantProgram.mutated.asInstanceOf[SparkRDDProgram]
    val mutatedProgramSource = originalProgramSource.copy().asInstanceOf[SparkRDDProgramSource]
    mutatedProgramSource.removeProgram(originalProgram)
    mutatedProgramSource.addProgram(mutatedProgram)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case node if node.isEqual(originalProgram.tree) => mutatedProgram.tree
        case node                                       => super.apply(node)
      }
    }
    mutatedProgramSource.tree = transformer(mutatedProgramSource.tree)
    MutantProgramSource(originalProgramSource, mutatedProgramSource, mutantProgram)
  }

}