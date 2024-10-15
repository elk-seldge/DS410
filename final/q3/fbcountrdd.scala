import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

bject fbcountrdd{
    def main(args: Array[String]) = {
        val conf = new SparkConf().setAppName("fbcountrdd")  
        val sc = new SparkContext(conf)
        val result = FBCountRdd(sc)
        result.saveAsTextFile("fbcountrdd")
    } 

    def FBCountRdd (sc: SparkContext) = {
        val datas = sc.textFile("/datasets/facebook").map(line => line.split(" "))
        val data = datas.filter(data => data._2 > 500) // filter to get right > 500
        val result = data.reduceByKey((left, value) => left += 1)
        result
        
    }

}


