package me.mentholi.tweetstream

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import twitter4j.Status

import scala.io.{AnsiColor, Source}

trait TwitterExampleBase {

  // Some type aliases to give a little bit of context
  type Tweet = Status
  type TweetText = String
  type Sentence = Seq[String]

  // First, let's configure Spark
  // We will deploy locally using a thread for each core (that's what the '*' stands for)
  val sparkConfiguration = new SparkConf().
    setAppName("spark-tweetstream").
    setMaster("local[*]")

  // Let's create the Spark Context using the configuration we just created
  val sparkContext = new SparkContext(sparkConfiguration)

  // Now let's wrap the context in a streaming one, passing along the window size
  val streamingContext = new StreamingContext(sparkContext, Seconds(2))

  // Creating a stream from Twitter (see the README to learn how to configure it)
  val tweets: DStream[Status] =
    TwitterUtils.createStream(streamingContext, None)

  // Load our word lists
  val uselessWords = load("/stop-words.dat")
  val positiveWords = load("/pos-words.dat")
  val negativeWords = load("/neg-words.dat")

  /**
    * Returns array of strings (words) from tweet
    */
  def wordsOf(tweet: TweetText): Sentence =
    tweet.split(" ")

  /**
    * Convert Sequence to lower case helper
    */
  def toLowercase(sentence: Sentence): Sentence =
    sentence.map(_.toLowerCase)

  /**
    * Helper to match actual words from the tweet
    */
  def keepActualWords(sentence: Sentence): Sentence =
    sentence.filter(_.matches("[a-z]+"))

  /**
    * Filter out words that we dont want to see in results
    */
  def keepMeaningfulWords(sentence: Sentence): Sentence =
    sentence.filter(!uselessWords.contains(_))

  /**
    * Helper to extract words from the sentence.
    */
  def extractWords(sentence: Sentence): Sentence =
    sentence.map(_.toLowerCase).filter(_.matches("[a-z]+"))

  /**
    * Computer score for word in tweet. If there is positive words
    * it will get positive score and if there is negative words
    * it will get negative score. If none of the above can be found
    * then we return score 0
    */
  def computeWordScore(word: String): Int =
    if (positiveWords.contains(word))       1
    else if (negativeWords.contains(word)) -1
    else                                    0

  /**
    * Compute total score for the tweet by summing all the
    * word scores on the tweet.
    */
  def computeSentenceScore(words: Sentence): Int =
    words.map(computeWordScore).sum

  /**
    * Helper to load words from .dat files to be used
    * as list of positive or negative words.
    */
  def load(resourcePath: String): List[String] = {
    val source = Source.fromInputStream(getClass.getResourceAsStream(resourcePath))
    val words = source.getLines.toList
    source.close()
    words
  }

  private def format(n: Int): String = f"$n%2d"

  private def wrapScore(s: String): String = s"[ $s ] "

  private def makeReadable(n: Int): String =
    if (n > 0)      s"${AnsiColor.GREEN + format(n) + AnsiColor.RESET}"
    else if (n < 0) s"${AnsiColor.RED   + format(n) + AnsiColor.RESET}"
    else            s"${format(n)}"

  private def makeReadable(s: String): String =
    s.takeWhile(_ != '\n').take(160) + "..."

  def makeReadable(sn: (String, Int)): String =
    sn match {
      case (tweetText, score) => s"${wrapScore(makeReadable(score))}${makeReadable(tweetText)}"
    }

}
