package com.esri.sparkstreaming

trait SimpleFeatureTrackPurger extends Serializable {
  def purge(track: SimpleFeatureTrack): Unit
}

case class MaxSimpleFeaturesPerTrackPurger(maxFeatures: Int) extends SimpleFeatureTrackPurger {
  override def purge(track: SimpleFeatureTrack): Unit = {
    if (track.size > maxFeatures)
      track.remove(track.oldest.get)
  }
}
