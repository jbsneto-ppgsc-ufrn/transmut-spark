package br.ufrn.dimap.forall.transmut.model

import DirectionsEnum._

trait Edge extends Element {

  def dataset: Dataset

  def transformation: Transformation

  def direction: DirectionsEnum

}