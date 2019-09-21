package br.ufrn.dimap.forall.model

import scala.collection.mutable.ListBuffer
import scala.meta.Tree

case class BinaryTransformation(override val id: Long) extends Transformation(id) {

  def this(id: Long, name: String, source: TreeElement) {
    this(id)
    _name = name
    _source = source
  }

  private var _name: String = _
  private var _source: TreeElement = _
  private var _edges: ListBuffer[Edge] = scala.collection.mutable.ListBuffer.empty[Edge]
  private var _firstInputEdge: Option[Edge] = None
  private var _secondInputEdge: Option[Edge] = None
  private var _outputEdge: Option[Edge] = None

  def name = _name
  def name_=(name: String) {
    _name = name
  }

  def source = _source
  def source_=(source: TreeElement) {
    _source = source
  }

  def firstInputEdge = if (_firstInputEdge.isDefined) _firstInputEdge else edges.headOption
  def secondInputEdge = if (_secondInputEdge.isDefined) _secondInputEdge else edges.tail.headOption
  def outputEdge = if (_outputEdge.isDefined) _outputEdge else edges.lastOption

  def firstInputDataset: Option[Dataset] = if (firstInputEdge.isDefined) Some(firstInputEdge.get.dataset) else None
  def secondInputDataset: Option[Dataset] = if (secondInputEdge.isDefined) Some(secondInputEdge.get.dataset) else None
  def outputDataset: Option[Dataset] = if (outputEdge.isDefined) Some(outputEdge.get.dataset) else None

  def edges = _edges.toList

  def addEdge(edge: Edge) {
    _edges += edge
  }

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

object BinaryTransformation {
  def apply(id: Long, name: String, source: TreeElement) = new BinaryTransformation(id, name, source)
}