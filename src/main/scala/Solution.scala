import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, count, first, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Solution {

  def solution1(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    // 1. Group a list of values to the key: Key => [V1, V2, V3...]
    val df1 = df.groupBy($"Key").agg(collect_list($"Value") as "Values")

    // 2. Count the most frequent values in each list
    val findMostCommon = udf { values: Seq[String] => {
      // Max[(v1, 3), (v2, 2), (v3, 10)...]
      values.foldLeft(Map.empty[String, Int].withDefaultValue(0)) {
        case (m, v) => m.updated(v, m(v) + 1)
      }.maxBy(_._2)._1
    }
    }
    df1.withColumn("Values", findMostCommon($"Values")).withColumnRenamed("Values", "Most Common")
  }

  def solution2(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    def windowSpec = Window.partitionBy("Key", "Value")         //defining a window frame for the aggregation
    df.withColumn("Count", count("Value").over(windowSpec))     // counting repetition of value for each group of key, value and assigning that value to new column called as count
      .orderBy($"Count".desc)                                                                   // order dataframe with count in descending order
      .groupBy("Key")                                                                     // group by key
      .agg(first("Value").as("Most Common"))                                //taking the first row of each key with count column as the most common key
  }

  def solution3(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    val df2 = df.groupBy("Key", "Value")    // Key, Value, count
      .count()

    df2.orderBy($"count".desc)               // Group by key, order the values descend, and return the most common value of each group
      .groupBy("Key")
      .agg(first("Value").as("Most Common"))
  }

  def resolve(spark: SparkSession, df: DataFrame, which: String) = {
    which match {
      case "1" => solution1(spark, df)
      case "2" => solution2(spark, df)
      case "3" => solution3(spark, df)
    }
  }

  /**
    * There are 3 solutions totally.
    * Run with
    *   1) "scala Solution src/main/resources/input.txt src/main/resources/outputs/result1 1" for the first solution
    *   2) "scala Solution src/main/resources/input.txt src/main/resources/outputs/result2 2" for the second solution
    *   3) "scala Solution src/main/resources/input.txt src/main/resources/outputs/result3 3" for the third solution
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val input = args.apply(0)
    val output = args.apply(1)
    val which = args.apply(2)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Solution")
      .getOrCreate()

    val df = spark.read.csv(input).toDF("Key", "Value")

    val res = resolve(spark, df, which)

    res.coalesce(1).write.csv(output)
  }

}
