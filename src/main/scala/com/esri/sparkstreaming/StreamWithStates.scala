package com.esri.sparkstreaming

import com.esri.arcgis.st.geometry.Point
import com.esri.arcgis.st.spark.GenericFeatureSchemaRDD
import com.esri.arcgis.st.time.Time
import com.esri.arcgis.st.{BoundedValue, ExtendedInfo, Feature, FeatureSchema}
import com.esri.realtime.analysis.tool.buffer.BufferCreator
import com.esri.realtime.analysis.tool.geometry.Projector
import com.esri.realtime.core.registry.ToolRegistry
import com.esri.realtime.core.tool.SimpleTool
import com.esri.sparkstreaming.Defaults._
import com.esri.sparkstreaming.FeatureFunctions.FeatureAttributeReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.format.DateTimeFormat
import scala.annotation.meta.param

case class DStreamTransformer(stream: DStream[Feature], featureSchema: FeatureSchema, tool: SimpleTool) {

  def transformSchema: FeatureSchema = tool.transformSchema(featureSchema)

  def transform: DStream[Feature] = stream.transform(rdd => {
    val extendedInfo: Option[ExtendedInfo] = Option(ExtendedInfo(BoundedValue(Long.MaxValue)))
    val featureSchemaRDD: GenericFeatureSchemaRDD = new GenericFeatureSchemaRDD(rdd, featureSchema, extendedInfo)
    tool.execute(featureSchemaRDD)
  })
}

case class FeatureState(trackId: String, track: FeatureTrack)

case class StreamWithStates(@(transient @param) stream: DStream[Feature], @(transient @param) states: DStream[(String, FeatureState)]) {

  def showTempView(viewName: String): Unit = {
    states.foreachRDD((rdd, time) => {
      val sqlContext: SQLContext = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate().sqlContext
      import sqlContext.implicits._

      val df: DataFrame = rdd.map {
        case (flightId, flightState) => (flightId, flightState.track.size)
      }.toDF(colNames = "flightId", "count")

      // Create a SQL table from this DataFrame
      df.createOrReplaceTempView(viewName = viewName)

      // Dump out the results - you can do any SQL you want here.
      val featureTracksDataFrame = sqlContext.sql(s"select * from $viewName")
      println(s"========= $viewName $time =========")
      featureTracksDataFrame.show()
    })
  }
}

object StreamWithStates {

  def apply(featureStream: DStream[Feature], featureSchema: FeatureSchema): StreamWithStates = {

    featureStream.checkpoint(Seconds(1))
    val featuresWithTrackIds = featureStream.map(feature => (feature.trackId(featureSchema), feature))

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

    StreamWithStates(featureStream, states)
  }
}

object FeatureStreamWithStatesRunner {

  val flightsSchema: FeatureSchema = FeatureSchema(
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
    val time = Time(DateTimeFormat.forPattern("MM/dd/yyyy hh:mm:ss a").parseDateTime(columns(1)).getMillis)

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

    val rootLogger: Logger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    def createStreamingContext(): StreamingContext = {
      val sparkConf: SparkConf = new SparkConf()
      sparkConf.setAppName("FeatureStreamWithStates")
      sparkConf.setMaster("local[4]")
      val ssc: StreamingContext = new StreamingContext(sparkConf, Duration(1000))
      ssc.checkpoint(checkpointDirectory)

      val socketStream: DStream[String] = ssc.socketTextStream(hostname = tcpHost, port = tcpPort)
      val flights: DStream[Feature] = socketStream.map(adaptFlight)

      // Flights State
      val flightsWithStates: StreamWithStates = StreamWithStates(flights, flightsSchema)
      flightsWithStates.showTempView("Flights")

      val flightsProjector: DStreamTransformer = DStreamTransformer(
        stream = flights,
        featureSchema = flightsSchema,
        tool = (ToolRegistry.get(Projector.definition.name) match {
          case Some(toolDef) => toolDef.newInstance(
            Map(
              Projector.Property.OutSr -> 3857
            )
          )
          case None => null
        }).asInstanceOf[SimpleTool]
      )

      // Projected Flights State
      val projectedFlightsWithStates: StreamWithStates = StreamWithStates(flightsProjector.transform, flightsProjector.transformSchema)
      projectedFlightsWithStates.showTempView("ProjectedFlights")

      val flightsBufferCreator: DStreamTransformer = DStreamTransformer(
        stream = flightsProjector.transform,
        featureSchema = flightsProjector.transformSchema,
        tool = (ToolRegistry.get(BufferCreator.definition.name) match {
          case Some(toolDef) => toolDef.newInstance(
            Map(
              BufferCreator.Property.BufferBy -> "Distance",
              BufferCreator.Property.Distance -> "100 meters",
              BufferCreator.Property.Method -> "Geodesic"
            )
          )
          case None => null
        }).asInstanceOf[SimpleTool]
      )

      // Projected and Buffered Flights State
//      val projectedAndBufferedFlightsWithStates: StreamWithStates = StreamWithStates(flightsBufferCreator.transform, flightsBufferCreator.transformSchema)
//      projectedAndBufferedFlightsWithStates.showTempView("ProjectedAndBufferedFlights")

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
