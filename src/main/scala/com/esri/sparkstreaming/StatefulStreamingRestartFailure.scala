package com.esri.sparkstreaming

import com.esri.sparkstreaming.Defaults._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import scala.annotation.meta.param

abstract class Geometry extends Serializable
class Point(x: Double, y: Double) extends Geometry {
  override def toString: String = s"($x, $y)"
}

trait HasGeometry {
  def geometry: Point
}

trait Feature extends HasGeometry with Serializable {
  def trackId: String
}

case class Flight(trackId: String, flightTime: String,
                  longitude: Double, latitude: Double,
                  origin: String, destination: String,
                  aircraft: String, altitude: Long, geometry: Point) extends Feature

case class FlightState(id: String, feature: Feature)

case class StatefulStream(@(transient @param) parent: DStream[Feature], @(transient @param) states: DStream[(String, FlightState)])

object StatefulStream {

  def apply(flights: DStream[Feature]): StatefulStream = {

    val flightsWithTrackIds = flights.map(flight => (flight.trackId, flight))

    // We'll define our state using our trackStateFunc function called updateFeatureTrack above, and also specify a session timeout value of 30 minutes.
    val stateSpec = StateSpec.function((trackId: String, flightOpt: Option[Feature], state: State[FlightState]) => {
      var flightState = state.getOption.getOrElse(FlightState(trackId, null))
      flightOpt foreach {
        feature: Feature => flightState = flightState.copy(feature = feature)
      }
      state.update(flightState)
      flightState
    })

    // Process features through StateSpec to update the state
    val flightsWithState = flightsWithTrackIds.mapWithState(stateSpec)

    // Take a snapshot of the current state so we can look at it
    val flightStates: DStream[(String, FlightState)] = flightsWithState.stateSnapshots()
    flightStates.checkpoint(Seconds(1)) // enables state recovery on restart

    StatefulStream(flights, flightStates)
  }
}

object StatefulStreamingRestartFailure {

  def main(args: Array[String]): Unit = {

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    def createStreamingContext(): StreamingContext = {
      val sparkConf = new SparkConf()
      sparkConf.setAppName("StatefulStreamingRestartFailure")
      sparkConf.setMaster("local[4]")
      val ssc = new StreamingContext(sparkConf, Duration(1000))
      ssc.checkpoint(checkpointDirectory)
      ssc
    }

    // Get StreamingContext from checkpoint data or create the context with a 1 second batch size
    // See [https://spark.apache.org/docs/2.4.1/streaming-programming-guide.html#checkpointing] for more details
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, createStreamingContext)

    val socketStream: DStream[String] = ssc.socketTextStream(hostname = tcpHost, port = tcpPort)
    socketStream.checkpoint(Seconds(1))

    val flights: DStream[Feature] = socketStream.map(s => {
      val columns: Array[String] = s.split(",").map(_.trim)
      Flight(
        columns(0),           // flightId
        columns(1),           // flightTime
        columns(2).toDouble,  // longitude
        columns(3).toDouble,  // latitude
        columns(4),           // origin
        columns(5),           // destination
        columns(6),           // aircraft
        columns(7).toLong,     // altitude
        new Point(columns(2).toDouble, columns(3).toDouble)
      ).asInstanceOf[Feature]
    })

    val sqlContext: SQLContext = SQLContextSingleton.getInstance(ssc.sparkContext)
    import sqlContext.implicits._

    // Process each RDD from each batch as it comes in
    val sStream = StatefulStream(flights)
    sStream.states.foreachRDD((rdd, time) => {
      val df = rdd.map {
        case (flightId, flightState) => (flightId, flightState.feature.geometry.toString)
      }.toDF("flightId", "geometry")

      // Create a SQL table from this DataFrame
      df.createOrReplaceTempView("flights")

      // Dump out the results - you can do any SQL you want here.
      val featureTracksDataFrame = sqlContext.sql(s"select * from flights")
      println(s"========= Flights $time =========")
      featureTracksDataFrame.show()
    })

    println("Starting execution...")
    // add the graceful shutdown hook
    sys.ShutdownHookThread {
      println("Stopping execution...")
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
