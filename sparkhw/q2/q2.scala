import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object q2 { 

  def main(args: Array[String]) = {
    val cmd_arg = args(0)
    val y = cmd_arg.toInt
    val conf = new SparkConf().setAppName("q2")
    val sc = new SparkContext(conf)
    val result = findHubs(sc, y)
    result.saveAsTextFile("sparkhw-q2")
  }

  def findHubs(sc: SparkContext, y: Int) = {
    val datas = sc.textFile("/datasets/flight/flight.csv").filter{line => !line.startsWith("ITIN_ID")}.map(line=> line.split(",")) // filter out the 1st line
    val total = datas.map(line => (line(3), line(5))).distinct // (org, dest) distinct
    val result = total.aggregateByKey(0)((state, v) => state + 1, _+_).filter(data => data._2 >= y)
    result
  }
 
}
