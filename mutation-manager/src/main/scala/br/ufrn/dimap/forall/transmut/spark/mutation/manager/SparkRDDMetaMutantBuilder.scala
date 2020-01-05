package br.ufrn.dimap.forall.transmut.spark.mutation.manager

import scala.meta._
import scala.meta.contrib._

import br.ufrn.dimap.forall.transmut.model.Program
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.manager.MetaMutantBuilder
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgram
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgram
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDProgram
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDProgramSource

object SparkRDDMetaMutantBuilder extends MetaMutantBuilder {

  def buildMetaMutantProgramSourceFromMutantProgramSources(programSource: ProgramSource, mutants: List[MutantProgramSource]): MetaMutantProgramSource = {
    val sparkRDDProgramSource = programSource.asInstanceOf[SparkRDDProgramSource]
    val mutatedSparkRDDProgramSource = sparkRDDProgramSource.copy().asInstanceOf[SparkRDDProgramSource]
    val programs = programSource.programs
    val mutantPrograms = mutants.map(m => m.mutantProgram)
    val metaMutantPrograms = scala.collection.mutable.ArrayBuffer.empty[MetaMutantProgram]
    for (prog <- programs) {
      val mutantsProg = mutantPrograms.filter(p => p.original == prog)
      val metaMutantProg = buildMetaMutantProgramFromMutantPrograms(prog, mutantsProg)
      metaMutantPrograms += metaMutantProg
      val transformer = new Transformer {
        override def apply(tree: Tree): Tree = tree match {
          case node if node.isEqual(prog.tree) => metaMutantProg.mutated.tree
          case node                            => super.apply(node)
        }
      }
      mutatedSparkRDDProgramSource.tree = transformer(mutatedSparkRDDProgramSource.tree)
    }
    MetaMutantProgramSource(sparkRDDProgramSource, mutatedSparkRDDProgramSource, mutants, metaMutantPrograms.toList)
  }

  def buildMetaMutantProgramFromMutantPrograms(program: Program, mutants: List[MutantProgram]): MetaMutantProgram = {
    val sparkRDDProgram = program.asInstanceOf[SparkRDDProgram]
    val mutatedSparkRDDProgram = sparkRDDProgram.copy().asInstanceOf[SparkRDDProgram]
    val mutatedBody = buildMetaMutantMatchExpression(sparkRDDProgram, mutants)
    val transformer = new Transformer {
      override def apply(tree: Tree): Tree = tree match {
        case q"..$mods def $methodName[..$tparams](...$paramss): $decltpe = $body" if methodName.value == sparkRDDProgram.name =>
          q"..$mods def $methodName[..$tparams](...$paramss): $decltpe = $mutatedBody"
        case node => super.apply(node)
      }
    }
    mutatedSparkRDDProgram.tree = transformer(mutatedSparkRDDProgram.tree)
    MetaMutantProgram(sparkRDDProgram, mutatedSparkRDDProgram, mutants)
  }

  private def buildMetaMutantMatchExpression(program: SparkRDDProgram, mutants: List[MutantProgram]): Term.Match = {
    val matchValue = q"""System.getProperty("CURRENT_MUTANT")"""
    val defaultProgramCase = buildDefaultProgramCaseFromOriginalProgram(program)
    val mutantProgramCases = mutants.map(mutantProgram => buildMutantProgramCaseFromMutantProgram(mutantProgram))
    val cases = mutantProgramCases :+ defaultProgramCase
    Term.Match(matchValue, cases)
  }

  private def buildDefaultProgramCaseFromOriginalProgram(program: SparkRDDProgram): Case = {
    val pattern: Pat = Pat.Wildcard() // "_"
    var expression: Term = q"{}"
    program.tree.traverse {
      case q"..$mods def $methodName[..$tparams](...$paramss): $decltpe = $body" if methodName.value == program.name => {
        expression = body
      }
    }
    Case(pattern, None, expression)
  }

  private def buildMutantProgramCaseFromMutantProgram(mutantProgram: MutantProgram): Case = {
    val id: Lit = Lit.String(mutantProgram.id.toString())
    val pattern: Pat = p"$id"
    var expression: Term = q"{}"
    mutantProgram.mutated.tree.traverse {
      case q"..$mods def $methodName[..$tparams](...$paramss): $decltpe = $body" if methodName.value == mutantProgram.original.name => {
        expression = body
      }
    }
    Case(pattern, None, expression)
  }

}