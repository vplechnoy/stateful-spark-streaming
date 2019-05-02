package com.esri.sparkstreaming

abstract class SimpleGeometry extends Serializable

trait HasSimpleGeometry {
  def geometry: SimplePoint
}

class SimplePoint(x: Double, y: Double) extends SimpleGeometry {
  override def toString: String = s"($x, $y)"
}
