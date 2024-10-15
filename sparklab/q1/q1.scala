import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object q1 {

    def main(args: Array[String]) = {
        val conf = new SparkConf().setAppName("wc")
        val sc = new SparkContext(conf)
        val input = sc.textFile("/ds410/warandpeace")
        val words = input.flatMap(_.split(" "))
        val kv = words.map(word => (word.length,1))
        val counts = kv.reduceByKey((x,y) => x+y)
        counts.saveAsTextFile("sparklab-q1q1")
}
}

