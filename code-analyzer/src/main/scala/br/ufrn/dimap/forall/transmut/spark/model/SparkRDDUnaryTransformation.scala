package br.ufrn.dimap.forall.transmut.spark.model

import br.ufrn.dimap.forall.transmut.model.Dataset
import br.ufrn.dimap.forall.transmut.model.Edge
import scala.meta.Tree

case class SparkRDDUnaryTransformation(override val id: Long) extends SparkRDDTransformation(id) {

  def this(id: Long, _name: String, _params: List[Tree], _source: Tree) {
    this(id)
    name = _name
    params = _params
    source = _source
  }

  private var _inputEdge: Option[Edge] = None
  private var _outputEdge: Option[Edge] = None

  def addInputEdge(edge: Edge) {
    addEdge(edge)
    _inputEdge = Some(edge)
  }

  def addOutputEdge(edge: Edge) {
    addEdge(edge)
    _outputEdge = Some(edge)
  }

  def inputEdge = if (_inputEdge.isDefined) _inputEdge else edges.headOption
  def outputEdge = if (_outputEdge.isDefined) _outputEdge else edges.lastOption

  def inputDataset: Option[Dataset] = if (inputEdge.isDefined) Some(inputEdge.get.dataset) else None
  def outputDataset: Option[Dataset] = if (outputEdge.isDefined) Some(outputEdge.get.dataset) else None

}

object SparkRDDUnaryTransformation {
  def apply(id: Long, name: String, params: List[Tree], source: Tree) = new SparkRDDUnaryTransformation(id, name, params, source)
  def apply(id: Long, name: String, source: Tree) = new SparkRDDUnaryTransformation(id, name, List(), source)
}