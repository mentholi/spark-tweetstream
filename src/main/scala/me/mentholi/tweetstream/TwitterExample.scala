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

  // Calculate most popular words for time window of 30 seconds
  // each 5 seconds
  val wordLists = textAndMeaningfulSentences.map(tweet => tweet._2)
  val wordPairs = wordLists
    .flatMap(_.tail)
    .filter(word => word.length > 3)
    .map(word => (word, 1))

  val windowedWordCounts = wordPairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(20), Seconds(4))
  windowedWordCounts.transform(rdd => rdd.sortBy(_._2, false)).print()

  // Now that the streaming is defined, start it
  streamingContext.start()

  // Let's await the stream to end - forever
  streamingContext.awaitTermination()

}
