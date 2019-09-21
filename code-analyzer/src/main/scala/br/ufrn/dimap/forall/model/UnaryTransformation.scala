package br.ufrn.dimap.forall.model

import scala.collection.mutable.ListBuffer
import scala.meta.Tree

case class UnaryTransformation(override val id: Long) extends Transformation(id) {

  def this(id: Long, name: String, params: List[Tree], source: TreeElement) {
    this(id)
    _name = name
    _params = params
    _source = source
  }

  private var _name: String = _
  private var _source: TreeElement = _
  private var _params: List[Tree] = _
  private var _edges: ListBuffer[Edge] = scala.collection.mutable.ListBuffer.empty[Edge]
  private var _inputEdge : Option[Edge] = None
  private var _outputEdge : Option[Edge] = None

  def name = _name
  def name_=(name: String) {
    _name = name
  }

  def source = _source
  def source_=(source: TreeElement) {
    _source = source
  }

  def params = _params
  def params_=(params: List[Tree]) {
    _params = params
  }
  
  def inputEdge = if(_inputEdge.isDefined) _inputEdge else edges.headOption
  def outputEdge = if(_outputEdge.isDefined) _outputEdge else edges.lastOption
  
  def inputDataset : Option[Dataset]  = if(inputEdge.isDefined) Some(inputEdge.get.dataset) else None
  def outputDataset : Option[Dataset] = if(outputEdge.isDefined) Some(outputEdge.get.dataset) else None
  
  def edges = _edges.toList

  def addEdge(edge: Edge) {
    _edges += edge
  }
  
  def addInputEdge(edge : Edge) {
    addEdge(edge)
    _inputEdge = Some(edge)
  }
  
  def addOutputEdge(edge : Edge) {
    addEdge(edge)
    _outputEdge = Some(edge)
  }
  
}

object UnaryTransformation {
  def apply(id: Long, name: String, params: List[Tree], source: TreeElement) = new UnaryTransformation(id, name, params, source)
}