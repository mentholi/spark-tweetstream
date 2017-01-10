package me.mentholi.tweetstream

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

object TwitterExample extends App with TwitterExampleBase {

  // Initialization and processing functions are defined in the TwitterExampleBase class

  // Let's extract the words of each tweet
  // We'll carry the tweet along in order to print it in the end
  val textAndSentences: DStream[(TweetText, Sentence)] =
    tweets.
      map(_.getText).
      map(tweetText => (tweetText, wordsOf(tweetText)))

  // Apply transformations that allow us to keep just interesting sentences
  val textAndMeaningfulSentences: DStream[(TweetText, Sentence)] =
    textAndSentences.
      mapValues(toLowercase).
      mapValues(keepActualWords).
      mapValues(keepMeaningfulWords).
      filter { case (_, sentence) => sentence.length > 0 }

  // Compute the score of each sentence and filter
  // out ones with neutral score.
  val textAndNonNeutralScore: DStream[(TweetText, Int)] =
    textAndMeaningfulSentences.
      mapValues(computeSentenceScore).
      filter { case (_, score) => score != 0 }

  // Transform the (tweet, score) tuple into a readable string and print it
  textAndNonNeutralScore.map(makeReadable).print

  // Flatten Array of words in each tweet to own items and then tuples of
  // word and integer. We also give them initial count of 1
  val wordStream = textAndSentences.flatMap(tweet => tweet._1.split(" "))
  val hashtagStream = wordStream
    .filter(w => w.startsWith("#"))
    .map(tag => (tag, 1))

  // Calculate most popular hashtags for past 20 seconds.
  // We re-calculate this list every 5 seconds.
  val windowedHashtagCounts = hashtagStream.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(60), Seconds(4))
  // According to some people in interwebs reduceByKeyAndWindow should return single rdd
  // so roting like this should work!
  windowedHashtagCounts.transform(rdd => rdd.sortBy(_._2, false)).print()

  // Save plain tweets to disk
  //tweets.saveAsTextFiles("/tmp/tweet_data", "txt")

  // Now that the streaming is defined, start it
  streamingContext.start()

  // Let's await the stream to end - forever
  streamingContext.awaitTermination()

}
