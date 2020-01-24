package br.ufrn.dimap.forall.transmut.mutation.manager

import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.model.Program
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgram
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgram
import java.io.File
import java.nio.file.Path
import br.ufrn.dimap.forall.transmut.util.IOFiles

trait MetaMutantBuilder {

  def buildMetaMutantProgramSourceFromMutantProgramSources(programSource: ProgramSource, mutants: List[MutantProgramSource]): MetaMutantProgramSource

  def buildMetaMutantProgramFromMutantPrograms(program: Program, mutants: List[MutantProgram]): MetaMutantProgram

  def writeMetaMutantToFile(metaMutant: MetaMutantProgramSource): Unit = writeMetaMutantToFile(metaMutant, metaMutant.mutated.source)

  def writeMetaMutantToFile(metaMutant: MetaMutantProgramSource, pathToWrite: Path): Unit = {
    IOFiles.writeContentToFile(pathToWrite.toFile(), metaMutant.mutated.tree.syntax)
  }

}