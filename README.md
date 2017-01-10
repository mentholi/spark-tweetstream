Spark Tweet stream example
===============

Spark streaming demo/experimentation app. This app streams Tweets from Twitter and ranks them based on some simple criteria. It also keeps track of most
popular hashtags on last minute by using `reduceByKeyAndWindow`.


# Developing

To make it work on your machine, be sure to add a `twitter4j.properties` under `src/main/resources` that includes the following information:

```
oauth.consumerKey=***
oauth.consumerSecret=***
oauth.accessToken=***
oauth.accessTokenSecret=***
```

Visit [apps.twitter.com](https://apps.twitter.com) to get your own API keys.


# Submitting to Spark

To submit the job to an existing Spark installation you can package the job with the following command:

```ShellSession
$ sbt assembly
```

To submit it use following command:

```ShellSession
$ $SPARK_HOME/bin/spark-submit \
   --class me.mentholi.TwitterExample \
   --master $SPARK_MASTER \
   target/scala-2.11/spark-tweetstream-assembly-0.0.1.jar
```


### Credits
This repo contains code from [https://github.com/stefanobaghino/spark-twitter-stream-example](https://github.com/stefanobaghino/spark-twitter-stream-example).