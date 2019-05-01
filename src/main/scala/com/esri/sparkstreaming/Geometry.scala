package com.esri.sparkstreaming

abstract class Geometry extends Serializable

trait HasGeometry {
  def geometry: Point
}

class Point(x: Double, y: Double) extends Geometry {
  override def toString: String = s"($x, $y)"
}
