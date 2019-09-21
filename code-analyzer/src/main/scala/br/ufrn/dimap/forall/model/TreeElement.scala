package br.ufrn.dimap.forall.model

import scala.meta._

case class TreeElement(override val id : Long, tree : Tree) extends Element(id)