package br.ufrn.dimap.forall.transmut.spark.model

import scala.meta.Tree

import br.ufrn.dimap.forall.transmut.model.Dataset
import br.ufrn.dimap.forall.transmut.model.Edge

case class SparkRDDBinaryTransformation(override val id: Long) extends SparkRDDTransformation(id) {
  
  def this(id: Long, _name: String, _params: List[Tree], _source: Tree) {
    this(id)
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
}

object SparkRDDBinaryTransformation {
  def apply(id: Long, name: String, params: List[Tree], source: Tree) = new SparkRDDBinaryTransformation(id, name, params, source)
  def apply(id: Long, name: String, source: Tree) = new SparkRDDBinaryTransformation(id, name, List(), source)
}