# stateful-spark-streaming
Simple test of stateful spark streaming

- Import apache maven project into IDE of your choice (IntelliJ IDEA)
- To build the project: mvn clean install
- Make sure folder '/checkpoint' exists and has writing permissions set for the user running the app
- Open new terminal session, navigate to [stateful-spark-streaming]/src/main/resources and execute the command: nc -kl 7777 < FlightSim.csv
- Run com.esri.sparkstreaming.StatefulStreamingRestartFailure class
- Observe server output similar to the following:
========= Flights 1556666752000 ms =========
+--------+--------------------+
|flightId|            geometry|
+--------+--------------------+
| SWA2706|(-81.950652333999...|
|    ASA3|(-102.28326338099...|
|  SWA724|(-75.427830957999...|
|  SWA992|(-87.999900149999...|
| SWA2358|(-78.392885559999...|
| SWA1568|(-102.06578643099...|
|    ASA6|(-104.46991687499...|
|    ASA2|(-106.51396253299...|
|  SWA510|(-77.227066495999...|
+--------+--------------------+
- Notice how checkpoint location is populated with multiple 'checkpoint-...' files 
- Stop application execution
- Rerun application
- Notice exception thrown:
19/04/30 15:48:56 ERROR streaming.StreamingContext: Error starting the context, marking it as stopped
org.apache.spark.SparkException: org.apache.spark.streaming.dstream.FlatMappedDStream@2e869098 has not been initialized
	at org.apache.spark.streaming.dstream.DStream.isTimeValid(DStream.scala:313)
	at org.apache.spark.streaming.dstream.DStream$$anonfun$getOrCompute$1.apply(DStream.scala:334)
	at org.apache.spark.streaming.dstream.DStream$$anonfun$getOrCompute$1.apply(DStream.scala:334)
	at scala.Option.orElse(Option.scala:289)
	at org.apache.spark.streaming.dstream.DStream.getOrCompute(DStream.scala:331)
	at org.apache.spark.streaming.dstream.ForEachDStream.generateJob(ForEachDStream.scala:48)
	at org.apache.spark.streaming.DStreamGraph$$anonfun$1.apply(DStreamGraph.scala:122)
	at org.apache.spark.streaming.DStreamGraph$$anonfun$1.apply(DStreamGraph.scala:121)
	at scala.collection.TraversableLike$$anonfun$flatMap$1.apply(TraversableLike.scala:241)
	at scala.collection.TraversableLike$$anonfun$flatMap$1.apply(TraversableLike.scala:241)
	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:48)
	at scala.collection.TraversableLike$class.flatMap(TraversableLike.scala:241)
	at scala.collection.AbstractTraversable.flatMap(Traversable.scala:104)
	at org.apache.spark.streaming.DStreamGraph.generateJobs(DStreamGraph.scala:121)
	at org.apache.spark.streaming.scheduler.JobGenerator$$anonfun$restart$4.apply(JobGenerator.scala:234)
	at org.apache.spark.streaming.scheduler.JobGenerator$$anonfun$restart$4.apply(JobGenerator.scala:229)
	at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33)
	at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186)
	at org.apache.spark.streaming.scheduler.JobGenerator.restart(JobGenerator.scala:229)
	at org.apache.spark.streaming.scheduler.JobGenerator.start(JobGenerator.scala:98)
	at org.apache.spark.streaming.scheduler.JobScheduler.start(JobScheduler.scala:103)
	at org.apache.spark.streaming.StreamingContext$$anonfun$liftedTree1$1$1.apply$mcV$sp(StreamingContext.scala:583)
	at org.apache.spark.streaming.StreamingContext$$anonfun$liftedTree1$1$1.apply(StreamingContext.scala:578)
	at org.apache.spark.streaming.StreamingContext$$anonfun$liftedTree1$1$1.apply(StreamingContext.scala:578)
	at ... run in separate thread using org.apache.spark.util.ThreadUtils ... ()
	at org.apache.spark.streaming.StreamingContext.liftedTree1$1(StreamingContext.scala:578)
	at org.apache.spark.streaming.StreamingContext.start(StreamingContext.scala:572)
	at com.esri.sparkstreaming.StatefulStreamingRestartFailure$.main(StatefulStreamingRestartFailure.scala:123)
	at com.esri.sparkstreaming.StatefulStreamingRestartFailure.main(StatefulStreamingRestartFailure.scala)
19/04/30 15:48:56 ERROR executor.Executor: Exception in task 0.0 in stage 1.0 (TID 1)
java.lang.IllegalArgumentException: There is already an RpcEndpoint called Receiver-0-1556664536441
	at org.apache.spark.rpc.netty.Dispatcher.registerRpcEndpoint(Dispatcher.scala:71)
	at org.apache.spark.rpc.netty.NettyRpcEnv.setupEndpoint(NettyRpcEnv.scala:130)
	at org.apache.spark.streaming.receiver.ReceiverSupervisorImpl.<init>(ReceiverSupervisorImpl.scala:75)
	at org.apache.spark.streaming.scheduler.ReceiverTracker$ReceiverTrackerEndpoint$$anonfun$9.apply(ReceiverTracker.scala:599)
	at org.apache.spark.streaming.scheduler.ReceiverTracker$ReceiverTrackerEndpoint$$anonfun$9.apply(ReceiverTracker.scala:591)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:2212)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:2212)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
	at org.apache.spark.scheduler.Task.run(Task.scala:121)
	at org.apache.spark.executor.Executor$TaskRunner$$anonfun$10.apply(Executor.scala:408)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1360)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:414)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
19/04/30 15:48:56 ERROR scheduler.TaskSetManager: Task 0 in stage 1.0 failed 1 times; aborting job
Exception in thread "main" org.apache.spark.SparkException: org.apache.spark.streaming.dstream.FlatMappedDStream@2e869098 has not been initialized
	at org.apache.spark.streaming.dstream.DStream.isTimeValid(DStream.scala:313)
	at org.apache.spark.streaming.dstream.DStream$$anonfun$getOrCompute$1.apply(DStream.scala:334)
	at org.apache.spark.streaming.dstream.DStream$$anonfun$getOrCompute$1.apply(DStream.scala:334)
	at scala.Option.orElse(Option.scala:289)
	at org.apache.spark.streaming.dstream.DStream.getOrCompute(DStream.scala:331)
	at org.apache.spark.streaming.dstream.ForEachDStream.generateJob(ForEachDStream.scala:48)
	at org.apache.spark.streaming.DStreamGraph$$anonfun$1.apply(DStreamGraph.scala:122)
	at org.apache.spark.streaming.DStreamGraph$$anonfun$1.apply(DStreamGraph.scala:121)
	at scala.collection.TraversableLike$$anonfun$flatMap$1.apply(TraversableLike.scala:241)
	at scala.collection.TraversableLike$$anonfun$flatMap$1.apply(TraversableLike.scala:241)
	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:48)
	at scala.collection.TraversableLike$class.flatMap(TraversableLike.scala:241)
	at scala.collection.AbstractTraversable.flatMap(Traversable.scala:104)
	at org.apache.spark.streaming.DStreamGraph.generateJobs(DStreamGraph.scala:121)
	at org.apache.spark.streaming.scheduler.JobGenerator$$anonfun$restart$4.apply(JobGenerator.scala:234)
	at org.apache.spark.streaming.scheduler.JobGenerator$$anonfun$restart$4.apply(JobGenerator.scala:229)
	at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33)
	at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186)
	at org.apache.spark.streaming.scheduler.JobGenerator.restart(JobGenerator.scala:229)
	at org.apache.spark.streaming.scheduler.JobGenerator.start(JobGenerator.scala:98)
	at org.apache.spark.streaming.scheduler.JobScheduler.start(JobScheduler.scala:103)
	at org.apache.spark.streaming.StreamingContext$$anonfun$liftedTree1$1$1.apply$mcV$sp(StreamingContext.scala:583)
	at org.apache.spark.streaming.StreamingContext$$anonfun$liftedTree1$1$1.apply(StreamingContext.scala:578)
	at org.apache.spark.streaming.StreamingContext$$anonfun$liftedTree1$1$1.apply(StreamingContext.scala:578)
	at ... run in separate thread using org.apache.spark.util.ThreadUtils ... ()
	at org.apache.spark.streaming.StreamingContext.liftedTree1$1(StreamingContext.scala:578)
	at org.apache.spark.streaming.StreamingContext.start(StreamingContext.scala:572)
	at com.esri.sparkstreaming.StatefulStreamingRestartFailure$.main(StatefulStreamingRestartFailure.scala:123)
	at com.esri.sparkstreaming.StatefulStreamingRestartFailure.main(StatefulStreamingRestartFailure.scala)