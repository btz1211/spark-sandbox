import org.apache.spark.sql.DataFrame

object RealEstateDataProcessor {
  implicit def toRealEstateDataFrame(df: DataFrame): RealEstateDataFrame = new RealEstateDataFrame(df)

  class RealEstateDataFrame(df : DataFrame) {

    def ofCity(city : String) : DataFrame = {
      df.where(s"city = '$city'")
    }

    def withPriceOver(price : Int) : DataFrame = {
      df.where(s"price > $price")
    }

    def withPriceUnder(price : Int) : DataFrame = {
      df.where(s"price < $price")
    }

    def withBedsGreaterOrEqual(beds : Int) : DataFrame = {
      df.where(s"beds >= $beds")
    }
  }
}
