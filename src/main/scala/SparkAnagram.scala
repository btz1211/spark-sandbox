import org.apache.spark.sql.SparkSession

object SparkAnagram {

  def main(args: Array[String]):Unit = {
    val filePath = args(0)
    val word = args(1)

    val spark = SparkSession.builder
      .appName("Spark Anagram")
      .config("spark.master", "local[2]")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile(filePath)
    rdd.map(word => (toSortedChars(word), word))
      .groupByKey()
      .filter(data => data._1.equals(toSortedChars(word)))
      .first()
      ._2.foreach(println)

    spark.close()
  }

  def toSortedChars(word : String) :String = {
    word.toLowerCase.toCharArray.sorted.mkString("")
  }
}
