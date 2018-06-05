import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamTest {
  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamTest")
    val ssc = new StreamingContext(conf, Seconds(5))

    /**
      * this stream listens on socket 9999, to publish data
      * run `nc -lk 9999` and start typing data
      */
    ssc.socketTextStream("localhost", 9999)
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .print()


    ssc.start()
    ssc.awaitTermination()

  }
}
