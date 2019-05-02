package com.esri.sparkstreaming

import com.esri.arcgis.st.geometry.Point
import com.esri.arcgis.st.time.{Time => ArcTime}
import com.esri.arcgis.st.{FeatureSchema, Feature}
import com.esri.sparkstreaming.FeatureFunctions.FeatureAttributeReader
import com.esri.sparkstreaming.Defaults._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.format.DateTimeFormat
import scala.annotation.meta.param

case class FeatureState(trackId: String, track: FeatureTrack)

case class FeatureStreamWithStates(@(transient @param) stream: DStream[Feature], @(transient @param) states: DStream[(String, FeatureState)])

object FeatureStreamWithStates {

  def apply(features: DStream[Feature])(implicit featureSchema: FeatureSchema): FeatureStreamWithStates = {

    val featuresWithTrackIds = features.map(feature => (feature.trackId, feature))

    // We'll define our state using our trackStateFunc function called updateFeatureTrack above, and also specify a session timeout value of 30 minutes.
    val featureStateSpec = StateSpec.function((trackId: String, featureOpt: Option[Feature], state: State[FeatureState]) => {
      val trackState: FeatureState = state.getOption.getOrElse(FeatureState(trackId, FeatureTrack(MaxFeaturesPerTrackPurger(10))))
      featureOpt map {
        feature: Feature => trackState.track add feature
      }
      state.update(trackState)
      trackState
    })

    // Process features through StateSpec to update the state
    val featuresWithState = featuresWithTrackIds.mapWithState(featureStateSpec)

    // Take a snapshot of the current state so we can look at it
    val states: DStream[(String, FeatureState)] = featuresWithState.stateSnapshots()
    states.checkpoint(Seconds(1)) // enables state recovery on restart

    FeatureStreamWithStates(features, states)
  }
}

object FeatureStreamWithStatesRunner {

  implicit val featureSchema: FeatureSchema = FeatureSchema(
    """
      |{
      |  "attributes": [
      |    {
      |      "name": "flightId",
      |      "dataType": "String",
      |      "nullable": false,
      |      "tags": [
      |        {
      |          "name": "TRACK_ID",
      |          "types": [
      |            "String"
      |          ]
      |        }
      |      ]
      |    },
      |    {
      |      "name": "flightTime",
      |      "dataType": "Date",
      |      "nullable": false,
      |      "tags": [
      |        {
      |          "name": "START_TIME",
      |          "types": [
      |            "Date"
      |          ]
      |        }
      |      ]
      |    },
      |    {
      |      "name": "longitude",
      |      "dataType": "Float64",
      |      "nullable": false,
      |      "tags": []
      |    },
      |    {
      |      "name": "latitude",
      |      "dataType": "Float64",
      |      "nullable": false,
      |      "tags": []
      |    },
      |    {
      |      "name": "origin",
      |      "dataType": "String",
      |      "nullable": false,
      |      "tags": []
      |    },
      |    {
      |      "name": "destination",
      |      "dataType": "String",
      |      "nullable": false,
      |      "tags": []
      |    },
      |    {
      |      "name": "aircraft",
      |      "dataType": "String",
      |      "nullable": false,
      |      "tags": []
      |    },
      |    {
      |      "name": "altitude",
      |      "dataType": "Int32",
      |      "nullable": false,
      |      "tags": []
      |    }
      |  ],
      |  "geometry": {
      |    "geometryType": "esriGeometryPoint",
      |    "spatialReference": {
      |      "wkid": 4326
      |    },
      |    "fieldName": "Geometry"
      |  },
      |  "time": {
      |    "timeType": "Instant"
      |  }
      |}
    """.stripMargin
  )

  private def adaptFlight(line: String): Feature = {
    val columns = line.replaceAll("\"", "").split(",")

    // parse the geometry
    val x: Double = java.lang.Double.parseDouble(columns(2))
    val y: Double = java.lang.Double.parseDouble(columns(3))
    val geometry = Point(x, y)
    val time = ArcTime(DateTimeFormat.forPattern("MM/dd/yyyy hh:mm:ss a").parseDateTime(columns(1)).getMillis)

    // parse attributes
    val attributes: Array[Any] = Array(
      columns(0),           // flightId
      columns(1),           // flightTime
      columns(2).toDouble,  // longitude
      columns(3).toDouble,  // latitude
      columns(4),           // origin
      columns(5),           // destination
      columns(6),           // aircraft
      columns(7).toLong     // altitude
    )
    Feature(attributes, geometry, time)
  }

  def main(args: Array[String]): Unit = {

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    def createStreamingContext(): StreamingContext = {
      val sparkConf = new SparkConf()
      sparkConf.setAppName("FeatureStreamWithStates")
      sparkConf.setMaster("local[4]")
      val ssc = new StreamingContext(sparkConf, Duration(1000))
      ssc.checkpoint(checkpointDirectory)

      val socketStream: DStream[String] = ssc.socketTextStream(hostname = tcpHost, port = tcpPort)
      socketStream.checkpoint(Seconds(1))
      val flights: DStream[Feature] = socketStream.map(adaptFlight)

      // Process each RDD from each batch as it comes in
      val sStream = FeatureStreamWithStates(flights)
      sStream.states.foreachRDD((rdd, time) => {
        val sqlContext: SQLContext = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate().sqlContext
        import sqlContext.implicits._

        val df = rdd.map {
          case (flightId, flightState) => (flightId, flightState.track.size)
        }.toDF("flightId", "count")

        // Create a SQL table from this DataFrame
        df.createOrReplaceTempView("flights")

        // Dump out the results - you can do any SQL you want here.
        val featureTracksDataFrame = sqlContext.sql(s"select * from flights")
        println(s"========= Flights $time =========")
        featureTracksDataFrame.show()
      })

      ssc
    }

    // Get StreamingContext from checkpoint data or create the context with a 1 second batch size
    // See [https://spark.apache.org/docs/2.4.1/streaming-programming-guide.html#checkpointing] for more details
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, createStreamingContext)

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
