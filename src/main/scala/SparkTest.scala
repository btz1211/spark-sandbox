import org.apache.spark.sql.SparkSession
import RealEstateDataProcessor._

object SparkTest {

  def main(args: Array[String]) : Unit = {
    val dataFile = "src/main/resources/real-estate-sample-data.csv"
    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local[2]")
      .getOrCreate()

    val df = spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(dataFile)

    df.printSchema()

    // uncomment for api example
//    df.ofCity("SACRAMENTO")
//    .withPriceOver(100000)
//    .withBedsGreaterOrEqual(2)
//    .show(5)

    // uncomment for conversion to Data Class example
//    import spark.implicits._
//    df.ofCity("SACRAMENTO")
//      .withPriceOver(100000)
//      .as[PropertyListing]
//      .show(5)


    spark.stop()
  }
}
