package br.ufrn.dimap.forall.transmut.spark.model

import scala.meta.Tree
import scala.meta.contrib._

import br.ufrn.dimap.forall.transmut.model.Dataset
import br.ufrn.dimap.forall.transmut.model.Edge
import br.ufrn.dimap.forall.transmut.model.Program

case class SparkRDDBinaryTransformation(override val id: Long, override val program: SparkRDDProgram) extends SparkRDDTransformation(id, program) {

  def this(id: Long, program: SparkRDDProgram, _name: String, _params: List[Tree], _source: Tree) {
    this(id, program)
    name = _name
    params = _params
    source = _source
  }

  private var _firstInputEdge: Option[Edge] = None
  private var _secondInputEdge: Option[Edge] = None
  private var _outputEdge: Option[Edge] = None

  def firstInputEdge = if (_firstInputEdge.isDefined) _firstInputEdge else edges.headOption
  def secondInputEdge = if (_secondInputEdge.isDefined) _secondInputEdge else edges.tail.headOption
  def outputEdge = if (_outputEdge.isDefined) _outputEdge else edges.lastOption

  def firstInputDataset: Option[Dataset] = if (firstInputEdge.isDefined) Some(firstInputEdge.get.dataset) else None
  def secondInputDataset: Option[Dataset] = if (secondInputEdge.isDefined) Some(secondInputEdge.get.dataset) else None
  def outputDataset: Option[Dataset] = if (outputEdge.isDefined) Some(outputEdge.get.dataset) else None

  def addFirstInputEdge(edge: Edge) {
    addEdge(edge)
    _firstInputEdge = Some(edge)
  }

  def addSecondInputEdge(edge: Edge) {
    addEdge(edge)
    _secondInputEdge = Some(edge)
  }

  def addOutputEdge(edge: Edge) {
    addEdge(edge)
    _outputEdge = Some(edge)
  }

  override def copy(id: Long = this.id, program: Program = this.program, name: String = this.name, source: Tree = this.source, params: List[Tree] = this.params, edges: List[Edge] = this.edges) = {
    var copyTransformation = SparkRDDBinaryTransformation(id, program.asInstanceOf[SparkRDDProgram], name, params, source)
    copyTransformation.edges = edges
    copyTransformation._firstInputEdge = this.firstInputEdge
    copyTransformation._secondInputEdge = this.secondInputEdge
    copyTransformation._outputEdge = this.outputEdge
    copyTransformation
  }

  override def equals(that: Any): Boolean = that match {
    case that: SparkRDDBinaryTransformation => {
      that.id == id &&
        that.program == program &&
        that.name == name &&
        that.source.isEqual(source) &&
        that.params == params &&
        that.edges == edges &&
        that.firstInputEdge == firstInputEdge &&
        that.secondInputEdge == secondInputEdge &&
        that.outputEdge == outputEdge
    }
    case _ => false
  }

}

object SparkRDDBinaryTransformation {
  def apply(id: Long, program: SparkRDDProgram, name: String, params: List[Tree], source: Tree) = new SparkRDDBinaryTransformation(id, program, name, params, source)
  def apply(id: Long, program: SparkRDDProgram, name: String, source: Tree) = new SparkRDDBinaryTransformation(id, program, name, List(), source)
}