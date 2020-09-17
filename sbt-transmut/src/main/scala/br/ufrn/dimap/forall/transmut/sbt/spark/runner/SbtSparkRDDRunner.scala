package br.ufrn.dimap.forall.transmut.sbt.spark.runner

import br.ufrn.dimap.forall.transmut.config.Config
import br.ufrn.dimap.forall.transmut.mutation.runner.MutantRunner
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.runner.TestResult
import br.ufrn.dimap.forall.transmut.mutation.runner.TestSuccess
import br.ufrn.dimap.forall.transmut.mutation.runner.TestError
import br.ufrn.dimap.forall.transmut.mutation.runner.TestFailed
import br.ufrn.dimap.forall.transmut.sbt.spark.TransmutSparkRDDPlugin.autoImport._

import sbt._
import sbt.Keys._

class SbtSparkRDDRunner(state: State)(implicit val config: Config) extends MutantRunner {

  private val settings: Seq[Def.Setting[_]] = Seq(
    fork in Test := true,
    scalaSource in Compile := config.transmutSrcDir.toFile())

  def runOriginalTest(programSource: ProgramSource): TestResult[ProgramSource] = {
    val extracted = Project.extract(state)
    val testState = extracted.appendWithSession(settings, state)
    val result = Project.runTask((transmutTest in Test), testState)
    val testResult = result match {
      case Some((_, Value(_))) => TestSuccess(programSource)
      case Some((_, Inc(_)))   => TestFailed(programSource)
      case _                   => TestError(programSource)
    }
    testResult
  }

  def runMutantTest(mutant: MutantProgramSource, equivalentMutants: List[Long], testOnlyLivingMutants: Boolean = false, livingMutants: List[Long] = List(), forceExecutionEnabled: Boolean = false, forceExecution: List[Long] = List()): TestResult[MutantProgramSource] = {
    if (!equivalentMutants.contains(mutant.id)) {
      if (testOnlyLivingMutants || forceExecutionEnabled) {
        if (livingMutants.contains(mutant.id) || forceExecution.contains(mutant.id)) {
          val extracted = Project.extract(state)
          val mutantSetting: Def.Setting[_] = javaOptions in Test += s"-DCURRENT_MUTANT=${String.valueOf(mutant.id)}"
          val mutantState = extracted.appendWithSession(settings :+ mutantSetting, state)
          val result = Project.runTask((transmutTest in Test), mutantState)
          val testResult = result match {
            case Some((_, Value(_))) => TestSuccess(mutant)
            case Some((_, Inc(_)))   => TestFailed(mutant)
            case _                   => TestError(mutant)
          }
          testResult
        } else {
          TestFailed(mutant)
        }
      } else {
        val extracted = Project.extract(state)
        val mutantSetting: Def.Setting[_] = javaOptions in Test += s"-DCURRENT_MUTANT=${String.valueOf(mutant.id)}"
        val mutantState = extracted.appendWithSession(settings :+ mutantSetting, state)
        val result = Project.runTask((transmutTest in Test), mutantState)
        val testResult = result match {
          case Some((_, Value(_))) => TestSuccess(mutant)
          case Some((_, Inc(_)))   => TestFailed(mutant)
          case _                   => TestError(mutant)
        }
        testResult
      }
    } else {
      TestSuccess(mutant)
    }
  }
}