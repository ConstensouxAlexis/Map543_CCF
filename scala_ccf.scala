import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

object ConnectedComponents {

  def main(args: Array[String]) {
    // Create a SparkSession
    val spark = SparkSession.builder.appName("ConnectedComponents").getOrCreate()

    // Load input data from a file named "web-Google.txt.gz"
    val inputFile = spark.sparkContext.textFile("C:/Users/alexi/Downloads/web-Google.txt.gz")

    // Filter out any lines that start with a '#' character
    val cleanInput = inputFile.filter(line => line(0).compare('#') > 0)

    // Define a function to perform the reduce operation
    def iterate(pair: (String, List[Iterable[String]])) : ListBuffer[Array[String]] = {
      var key = pair._1
      var valuesIterate = pair._2.flatten
      var min = pair._1
      var listValues = List.empty[String]
      var toReturn = new ListBuffer[Array[String]]()

      // Iterate over the values and find the minimum key
      for (value <- valuesIterate) {
        if (min < value) {
          min = value
        }
        listValues :+ value
      }

      // If the minimum key is less than the current key,
      // add a new key-value pair to the return list for each value
      if (min < key) {
        var keyMinPair = Array(key, min)
        toReturn += keyMinPair
        for (value <- listValues){
          if (min != value){
            newPairCounter.add(1)
            keyMinPair = Array(value, min)
            toReturn += keyMinPair
          }
        }
      }
      toReturn
    }

    // Create a long accumulator to count the number of new pairs generated
    val newPairCounter = spark.sparkContext.longAccumulator("newPairCounter")

    // Set the iteration count to 0
    var nbIter = 0

    // Split each edge into a pair of vertices and store them in a new RDD
    var splitted = cleanInput.map(edge => edge.split("\\s+"))

    // Iterate until there are no new pairs generated
    while (newPairCounter.value > 0) {
      nbIter += 1
      newPairCounter.value = 0

      // Map each vertex to a pair with its neighbor
      val mapped = splitted.map(x => ((x.head, x.tail(0))), (x.tail(0), x.head)).flatMap(x => Array(x._1, x._2))

      // Group the vertices by their keys and create a list of their values
      val shuffleSort = mapped.groupByKey().map(x => (x._1, List(x._2)))

      // Reduce the vertices and get a list of new pairs
      var reduce = shuffleSort.flatMap(pair => iterate(pair))
      var reduceDistinct = reduce.distinct

      // Show the new pairs
      reduceDistinct.show()

      // Reverse each pair and group them by their keys to get the connected components
      var return_ = reduceDistinct.map(e => e.reverse)
      val totalConnectedComponents = return_.groupByKey(x => x(0)).mapGroups({
        case(key:String, values:Iterator[Array[String]]) => (key, values.map(x => x(1)).toArray)
      })

      // Print the final result
      println("CCF terminated")
    }

    // Stop the SparkSession
    spark.stop()
  }
}
