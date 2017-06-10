/**
  * Created by ALINA on 10.06.2017.
  */

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkTwitterStreaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    // Configure your Twitter credentials
    val apiKey = "xxxx"
    val apiSecret = "xxx"
    val accessToken = "xxx"
    val accessTokenSecret = "xxx"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    
    // Create Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.map(t => t.getText)

    tweets.print()

    ssc.start()
    ssc.awaitTermination()
  }
}



