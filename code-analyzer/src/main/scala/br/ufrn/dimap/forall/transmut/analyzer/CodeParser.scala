package br.ufrn.dimap.forall.transmut.analyzer

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import scala.meta.Source
import scala.meta.Tree
import scala.meta.inputs.Input
import br.ufrn.dimap.forall.transmut.util.IOFiles

object CodeParser {

  def getTreeFromPath(codePath: Path): Tree = {
    val text = IOFiles.readContentFromFile(codePath.toFile())
    val input = Input.VirtualFile(codePath.toString, text)
    val tree: Tree = input.parse[Source].get
    tree
  }

  def getTreeFromPath(codePath: String): Tree = getTreeFromPath(Paths.get(codePath))

}