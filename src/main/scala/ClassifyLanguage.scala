import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ALINA on 27.10.2017.
  */

object ClassifyLanguage {

  //Featurize Function
  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

  def main(args: Array[String]): Unit = {

    val modelInput = args(0)

    System.setProperty("hadoop.home.dir", "F:\\HADOOP");

    val conf = new SparkConf()
    conf.setAppName("language-classifier")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    // Configure your Twitter credentials
    TwitterStream.InitializeTwitterStream()

    // Create Twitter Stream
    println("Initializing Twitter stream...")
    val tweets = TwitterUtils
      .createStream(ssc, None)

    val texts = tweets.map(_.getText)

    println("Initializing the KMeans model...")
    val model = KMeansModel.load(sc, modelInput)
    val langNumber = 3

    val filtered = texts.filter(t => model.predict(featurize(t)) == langNumber)
    filtered.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
