package br.ufrn.dimap.forall.transmut.spark.model

import br.ufrn.dimap.forall.transmut.model.Dataset
import br.ufrn.dimap.forall.transmut.model.Edge
import scala.meta.Tree

// Class created to represent other not supported RDD transformations or actions 
case class SparkRDDOperation (override val id: Long) extends SparkRDDTransformation(id) {

  def this(id: Long, _name: String, _params: List[Tree], _source: Tree) {
    this(id)
    name = _name
    params = _params
    source = _source
  }

}

object SparkRDDOperation {
  def apply(id: Long, name: String, params: List[Tree], source: Tree) = new SparkRDDOperation(id, name, params, source)
  def apply(id: Long, name: String, source: Tree) = new SparkRDDOperation(id, name, List(), source)
}