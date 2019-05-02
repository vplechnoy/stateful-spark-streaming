package com.esri.sparkstreaming

import java.text.{ParseException, SimpleDateFormat}

trait SimpleTime extends Serializable {

  def isEmpty: Boolean
  def start: SimpleInstant
  def end: SimpleInstant

  def after(other: SimpleTime): Boolean = SimpleTime.after(SimpleTime.this, other)
  def before(other: SimpleTime): Boolean = SimpleTime.before(SimpleTime.this, other)
}

trait HasSimpleTime {
  def time: SimpleTime
}

case class SimpleInstant(milliseconds: Long) extends SimpleTime {
  override def isEmpty = false
  override def start: SimpleInstant = this
  override def end: SimpleInstant = this

  def <(other: SimpleInstant): Boolean = milliseconds < other.milliseconds
}

object SimpleTime {

  val sdf: SimpleDateFormat = new SimpleDateFormat("MM/dd/yy HH:mm:ss")

  def apply(time: String): SimpleTime = {
    val milliseconds: Long = try {
      sdf.parse(time).getTime
    } catch {
      case _: ParseException => 0
    }
    SimpleInstant(milliseconds)
  }

  def isNullOrEmpty(time: SimpleTime): Boolean = time == null || time.isEmpty

  def after(x: SimpleTime, y: SimpleTime): Boolean =
    if (SimpleTime.isNullOrEmpty(x) || SimpleTime.isNullOrEmpty(y)) false
    else y.end < x.start

  def before(x: SimpleTime, y: SimpleTime): Boolean =
    if (SimpleTime.isNullOrEmpty(x) || SimpleTime.isNullOrEmpty(y)) false
    else x.end < y.start
}
