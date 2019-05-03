package com.esri.sparkstreaming

trait SimpleFeature extends HasSimpleGeometry with HasSimpleTime with Serializable {
  def trackId: String
}

case class Flight(trackId: String, flightTime: String,
                  longitude: Double, latitude: Double,
                  origin: String, destination: String,
                  aircraft: String, altitude: Long, time: SimpleTime, geometry: SimplePoint) extends SimpleFeature
