package me.mentholi.tweetstream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status
//import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.twitter.TwitterUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//import twitter4j.{GeoLocation, Status}

object TwitterStateExample extends App {

  val sparkConfiguration = new SparkConf().
    setAppName("spark-statefull-tweetstream").
    setMaster("local[*]")

  // Let's create the Spark Context using the configuration we just created
  val sparkContext = new SparkContext(sparkConfiguration)

  // Now let's wrap the context in a streaming one, passing along the window size
  val streamingContext = new StreamingContext(sparkContext, Seconds(2))
  streamingContext.checkpoint("/tmp/checkpoint/spark-statefull-tweetstream")

  // Creating a stream from Twitter (see the README to learn how to configure it)
  val tweets: DStream[Status] =
    TwitterUtils.createStream(streamingContext, None)

  val stateTrackingFunc = (screenName: String, one: Option[Int], state: State[Int]) => {
    val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (screenName, sum)
    state.update(sum)
    output
  }

  // Let's extract tweeter screen name as we care only about them now.
  // We map name to 1 so that we can easily count them
  val tweetLocations: DStream[(String, Int)] = tweets.map(tweet => (tweet.getUser().getScreenName(), 1))

  // Keep state and count how many tweets come from each country
  val stateDstream = tweetLocations.mapWithState(
    StateSpec.function(stateTrackingFunc).timeout(Minutes(30))
  )
  // Snapshot state and output users who have most tweets.
  val stateSnapshot = stateDstream.stateSnapshots()
  // Call repartition(1) before we sort the rdd so that we have single rdd to sort.
  val sortedSnapshot = stateSnapshot.repartition(1).transform(rdd => rdd.sortBy(_._2, false))
  // Print 20 users who have tweeted most
  sortedSnapshot.print(20)

  // Now that the streaming is defined, start it
  streamingContext.start()

  // Let's await the stream to end - forever
  streamingContext.awaitTermination()

}
