import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, collect_list, concat_ws, explode, lit}

object ConnectedComponents {

  def main(args: Array[String]) {
    // Create a SparkSession
    val spark = SparkSession.builder.appName("ConnectedComponents").getOrCreate()


    val inputFile = spark.read.text("dataset_filepath")

    // The input file is supposed to be a random data file
    // In our case, the rest of our code is suited for the the web-google.txt file,
    // A standard graph dataset that we used
    
    // Filter out any lines that start with a '#' character
    val cleanInput = inputFile.filter(col("value").substr(1, 1) =!= "#")

    // Split each edge into a pair of vertices and store them in a new DataFrame
    var splitted = cleanInput.select(concat_ws(",", col("value").substr(3), col("value").substr(1, 2)).alias("edge"))
      .selectExpr("split(edge, ',')[0] as src", "split(edge, ',')[1] as dst")

    // Define a function to perform the reduce operation
    def iterate(df: DataFrame) : DataFrame = {
      val minDf = df.groupBy("dst").agg(collect_list("src").as("srcs"))
        .withColumn("min", array(col("dst"), col("srcs")).flatten.min)
      val resultDf = df.join(minDf, col("dst") === col("min"), "inner")
        .select(col("src"), col("min").alias("dst"))
      resultDf
    }

    // Set the iteration count to 0
    var nbIter = 0

    // Iterate until there are no new pairs generated
    var df = splitted
    var newDf = iterate(df)
    while (!newDf.except(df).isEmpty) {
      nbIter += 1
      df = newDf
      newDf = iterate(df)
    }

    // Reverse each pair and group them by their keys to get the connected components
    val ccDf = df.union(newDf)
      .select(col("dst"), col("src"))
      .groupBy(col("dst"))
      .agg(collect_list(col("src")).alias("cc"))

    // Print the final result
    ccDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
