package com.esri.sparkstreaming

trait Feature extends HasGeometry with HasTime with Serializable {
  def trackId: String
}
