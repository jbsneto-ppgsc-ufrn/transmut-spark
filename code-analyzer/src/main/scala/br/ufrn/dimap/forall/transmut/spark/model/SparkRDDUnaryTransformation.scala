package br.ufrn.dimap.forall.transmut.spark.model

import br.ufrn.dimap.forall.transmut.model.Dataset
import br.ufrn.dimap.forall.transmut.model.Edge
import scala.meta.Tree
import scala.meta.contrib._

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

  override def copy(id: Long = this.id, name: String = this.name, source: Tree = this.source, params: List[Tree] = this.params, edges: List[Edge] = this.edges) = {
    var copyTransformation = SparkRDDUnaryTransformation(id, name, params, source)
    copyTransformation.edges = edges
    copyTransformation._inputEdge = this.inputEdge
    copyTransformation._outputEdge = this.outputEdge
    copyTransformation
  }

  override def equals(that: Any): Boolean = that match {
    case that: SparkRDDUnaryTransformation => {
      that.id == id &&
        that.name == name &&
        that.source.isEqual(source) &&
        that.params == params &&
        that.edges == edges &&
        that.inputEdge == inputEdge &&
        that.outputEdge == outputEdge
    }
    case _ => false
  }

}

object SparkRDDUnaryTransformation {
  def apply(id: Long, name: String, params: List[Tree], source: Tree) = new SparkRDDUnaryTransformation(id, name, params, source)
  def apply(id: Long, name: String, source: Tree) = new SparkRDDUnaryTransformation(id, name, List(), source)
}