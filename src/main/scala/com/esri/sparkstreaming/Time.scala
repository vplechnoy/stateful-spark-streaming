package com.esri.sparkstreaming

import java.text.{ParseException, SimpleDateFormat}

trait Time extends Serializable {

  def isEmpty: Boolean
  def start: Instant
  def end: Instant

  def after(other: Time): Boolean = Time.after(Time.this, other)
  def before(other: Time): Boolean = Time.before(Time.this, other)
}

trait HasTime {
  def time: Time
}

case class Instant(milliseconds: Long) extends Time {
  override def isEmpty = false
  override def start: Instant = this
  override def end: Instant = this

  def <(other: Instant): Boolean = milliseconds < other.milliseconds
}

object Time {

  val sdf: SimpleDateFormat = new SimpleDateFormat("MM/dd/yy HH:mm:ss")

  def apply(time: String): Time = {
    val milliseconds: Long = try {
      sdf.parse(time).getTime
    } catch {
      case _: ParseException => 0
    }
    Instant(milliseconds)
  }

  def isNullOrEmpty(time: Time): Boolean = time == null || time.isEmpty

  def after(x: Time, y: Time): Boolean =
    if (Time.isNullOrEmpty(x) || Time.isNullOrEmpty(y)) false
    else y.end < x.start

  def before(x: Time, y: Time): Boolean =
    if (Time.isNullOrEmpty(x) || Time.isNullOrEmpty(y)) false
    else x.end < y.start
}
