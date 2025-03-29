import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object PizzaApp {
  def main(args: Array[String]): Unit ={
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("pizza-application" )
      .master("local")
      .getOrCreate()

     import sparkSession.implicits._

    val pizzaRawDF: Dataset[Row] = sparkSession.read
      .option("header", "true")
      .csv("pizza_data.csv")

    val pizzaCleanDF: Dataset[Row] = pizzaRawDF.withColumn("Price", regexp_replace(col("Price"), "[$.]", "").cast(DataTypes.DoubleType))

    //val pizzaRoundDF: Dataset[Row] = pizzaCleanDF.withColumn("Price", round(col("Price"), 2))

    val companiseWithAvgPricesDF: Dataset[Row] = pizzaCleanDF.groupBy("Company").avg("Price")

    val companiseWithAvgPricesRoundDF: Dataset[Row] = companiseWithAvgPricesDF.withColumn("avg(Price)", round(col("avg(Price)"), 2))

    companiseWithAvgPricesRoundDF.show()
    companiseWithAvgPricesRoundDF.printSchema()
  }
}
