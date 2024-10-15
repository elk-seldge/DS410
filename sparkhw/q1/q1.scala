import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
/* ITIN_ID, 	YEAR, QUARTER, ORIGIN, ORIGIN_STATE_NM, 	DEST, 			DEST_STATE_NM, 	PASSENGERS
2021118,	2021, 1,	CAE,	South Carolina,		FLL,			Florida,	1.00
2021119,	2021, 1,	CAE,	South Carolina,		FLL,			Florida,	2.00
2021120,	2021, 1,	CAE,	South Carolina,		FLL,			Florida,	6.00
2021121,	2021, 1,	CAE,	South Carolina,		FLL,			Florida,	3.00 */
object Q1{
  def main(args: Array[String]) = {
    // call your code using spark-submit nameofjarfile.jar commandlinearg
    val conf = new SparkConf().setAppName("q1")  
    val sc = new SparkContext(conf)
    val cmd_arg = args(0) // this is the commandlinearg
    val x = cmd_arg.toInt
    val result = findPopular(sc, x)
    result.saveAsTextFile("sparkhw-q1")
    
  }
  def findPopular (sc: SparkContext, x: Int) = {
     val datas = sc.textFile("/datasets/flight").filter{line => !line.startsWith("ITIN_ID")}.map(line => line.split(",")) // filter out the 1st line
    // ["2021118", "2021", "1", "CAE", "South Carolina", "FLL", "Florida", "1.00"]
    //val arrv = datas.map{data => (data(5), data(7).toFloat)} // arrivals
    //val dprt = datas.map{data => (data(5), -data(7).toFloat)} // departures
    val total = datas.flatMap( line => Seq((line(3), -line(7).toDouble.toInt), (line(5), line(7).toDouble.toInt))) // [(CAE, -1), (FLL, 1)]
    val result = total.aggregateByKey(0)(_+_, _+_).filter(data => data._2 > x) //
    result

  }

}
