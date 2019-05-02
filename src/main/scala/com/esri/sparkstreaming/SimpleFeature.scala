package com.esri.sparkstreaming

trait SimpleFeature extends HasSimpleGeometry with HasSimpleTime with Serializable {
  def trackId: String
}
