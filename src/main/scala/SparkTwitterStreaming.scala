/**
  * Created by ALINA on 10.06.2017.
  */

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkTwitterStreaming {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "F:\\HADOOP");

    // Configure your Twitter credentials
    TwitterStream.InitializeTwitterStream()


    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))


    // Create Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.map(t => t.getText)
    tweets.print()

    tweets.saveAsTextFiles("F:\\data")
    //get hashtag
    val hashtags = tweets.flatMap(s => s.split(" ")).filter(w => w.startsWith("#"))
    hashtags.print()

    ssc.start()
    ssc.awaitTermination()
  }
}



