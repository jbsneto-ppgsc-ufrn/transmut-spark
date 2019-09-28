package br.ufrn.dimap.forall.transmut.spark.model

import scala.collection.mutable.ListBuffer

import br.ufrn.dimap.forall.transmut.model._
import scala.meta.Tree

case class SparkRDD(override val id: Long) extends Dataset {

  def this(id: Long, ref: Reference, sourc: Tree) {
    this(id)
    _reference = ref
    _source = sourc
  }

  private var _reference: Reference = _

  private var _source: Tree = _

  private var _edges: ListBuffer[Edge] = scala.collection.mutable.ListBuffer.empty[Edge]

  def reference = _reference

  def reference_=(reference: Reference) {
    _reference = reference
  }

  def source = _source

  def source_=(source: Tree) {
    _source = source
  }

  override def edges = _edges.toList

  def addEdge(edge: Edge) {
    _edges += edge
  }

}

object SparkRDD {
  def apply(id: Long, reference: Reference, source: Tree) = new SparkRDD(id, reference, source)
}