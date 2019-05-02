package com.esri.sparkstreaming

import java.util
import java.util.function.Predicate
import java.util.{Collections, Comparator, NoSuchElementException}
import scala.util.control.Breaks._

case class NumEntriesPredicate[SimpleFeature](var numEntries: Int) extends Predicate[SimpleFeature] with Serializable {
  override def test(feature: SimpleFeature): Boolean = {
    val testResult = numEntries > 0
    numEntries = if (testResult) numEntries - 1 else 0
    testResult
  }
}

case class SimpleFeatureTrack(purger: SimpleFeatureTrackPurger) extends Serializable {
  val track: util.SortedSet[SimpleFeature] = new util.TreeSet[SimpleFeature](SimpleFeatureOrder.chronological)
  def iterator: util.Iterator[SimpleFeature] = track.iterator
  def size: Int = track.size
  def isEmpty: Boolean = track.isEmpty

  def contains(feature: SimpleFeature): Boolean = {
    try {
      return track.contains(feature)
    } catch {
      case _: ClassCastException =>
    }
    false
  }

  def add(feature: SimpleFeature): Boolean = {
    if (!contains(feature) && track.add(feature)) {
      purger.purge(this)
      return true
    }
    false
  }

  def remove(feature: SimpleFeature): Boolean = {
    try {
      return track.remove(feature)
    } catch {
      case _: ClassCastException =>
    }
    false
  }

  def clear(): Unit = track.clear()

  def latest: Option[SimpleFeature] = Option(
    try {
      track.last
    } catch {
      case _: NoSuchElementException => null
    }
  )


  def oldest: Option[SimpleFeature] = Option(
    try {
      track.first
    } catch {
      case _: NoSuchElementException => null
    }
  )

  def previous(feature: SimpleFeature, predicate: NumEntriesPredicate[SimpleFeature] = NumEntriesPredicate(1), ordered: Comparator[SimpleFeature] = SimpleFeatureOrder.chronological): util.NavigableSet[SimpleFeature] = {
    val features = new util.TreeSet[SimpleFeature](ordered)
    val source = headSet(feature)
    if (source.size() >= predicate.numEntries) {
      try {
        val iterator = headSet(feature).descendingIterator()
        breakable {
          while(iterator.hasNext) {
            val f = iterator.next()
            if (predicate.test(f))
              features.add(f)
            else
              break
          }
        }
      } catch {
        case _: Exception =>
      }
    }
    features
  }

  def next(feature: SimpleFeature, predicate: NumEntriesPredicate[SimpleFeature] = NumEntriesPredicate(1), ordered: Comparator[SimpleFeature] = SimpleFeatureOrder.chronological): util.NavigableSet[SimpleFeature] = {
    val features = new util.TreeSet[SimpleFeature](ordered)
    val source = tailSet(feature)
    if (source.size() > predicate.numEntries) {
      try {
        val iterator = source.iterator()
        iterator.next()
        breakable {
          while(iterator.hasNext) {
            val f = iterator.next()
            if (predicate.test(f))
              features.add(f)
            else
              break
          }
        }
      } catch {
        case _: Exception =>
      }
    }
    features
  }

  private def headSet(feature: SimpleFeature): util.NavigableSet[SimpleFeature] = {
    try {
      return new util.TreeSet[SimpleFeature](track.headSet(feature))
    } catch {
      case _: Exception =>
    }
    Collections.emptyNavigableSet()
  }

  private def tailSet(feature: SimpleFeature): util.NavigableSet[SimpleFeature] = {
    try {
      return new util.TreeSet[SimpleFeature](track.tailSet(feature))
    } catch {
      case _: Exception =>
    }
    Collections.emptyNavigableSet()
  }

  private def subSet(from: SimpleFeature, to: SimpleFeature): util.NavigableSet[SimpleFeature] = {
    try {
      return new util.TreeSet[SimpleFeature](track.subSet(from, to))
    } catch {
      case _: Exception =>
    }
    Collections.emptyNavigableSet()
  }
}

trait SimpleFeatureOrder extends Comparator[SimpleFeature] with Serializable

class ChronologicalSimpleFeatureOrder extends SimpleFeatureOrder {
  override def compare(feature1: SimpleFeature, feature2: SimpleFeature): Int = {
    if (feature1.time.before(feature2.time))
      -1
    else if (feature1.time.after(feature2.time))
      1
    else
      0
  }
}

class ReversedSimpleFeatureOrder extends SimpleFeatureOrder {
  override def compare(feature1: SimpleFeature, feature2: SimpleFeature): Int = {
    if (feature1.time.before(feature2.time))
      1
    else if (feature1.time.after(feature2.time))
      -1
    else
      0
  }
}

object SimpleFeatureOrder {
  def chronological: Comparator[SimpleFeature] = new ChronologicalSimpleFeatureOrder()
  def reversed: Comparator[SimpleFeature] = new ReversedSimpleFeatureOrder()
}
