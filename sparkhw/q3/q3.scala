import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
/* ITIN_ID,     YEAR, QUARTER, ORIGIN, ORIGIN_STATE_NM,         DEST,                   DEST_STATE_NM,  PASSENGERS
2021118,        2021, 1,        CAE,    South Carolina,         FLL,                    Florida,        1.00
2021119,        2021, 1,        CAE,    South Carolina,         FLL,                    Florida,        2.00
2021120,        2021, 1,        CAE,    South Carolina,         FLL,                    Florida,        6.00
2021121,        2021, 1,        CAE,    South Carolina,         FLL,                    Florida,        3.00 */
object Q3{
  def main(args: Array[String]) = {
    // call your code using spark-submit nameofjarfile.jar commandlinearg
    val conf = new SparkConf().setAppName("q1")
    val sc = new SparkContext(conf)
    val cmd_arg1 = args(0) // this is the commandlinearg
    val cmd_arg2 = args(1)
    val x = cmd_arg1.toInt
    val y = cmd_arg2.toInt
    val result = findPopular_Hubs(sc, x, y)
    result.saveAsTextFile("sparkhw-q3")

  }

  def findPopular (sc: SparkContext, x: Int) = {
     val datas = sc.textFile("/datasets/flight").filter{line => !line.startsWith("ITIN_ID")}.map(line => line.split(",")) // filter out the 1st line
    // ["2021118", "2021", "1", "CAE", "South Carolina", "FLL", "Florida", "1.00"]
      val arrv = datas.flatMap{data => Seq(data(5), data(7).toDouble.toInt)} // arrivals
      val dprt = datas.flatMap{data => Seq(data(5), -data(7).toDouble.toInt)} // departures
      val arrv_agg = arrv.aggregateByKey(0)(_+_, _+_)
      val dprt_agg = dprt.aggregateByKey(0)(_+_, _+_)
      val total = arrv_agg.join(dprt_agg) // (state, (in, out))
      val result = total.filter(x => x._2._1 - x._2._2 > x)
      result

  }

  def findHubs(sc: SparkContext, y: Int) = {
    val datas = sc.textFile("/datasets/flight/flight.csv").filter{line => !line.startsWith("ITIN_ID")}.map(line=> line.split(",")) // filter out the 1st line
    val total = datas.map(line => (line(3), line(5))).distinct // (org, dest) distinct
    val result = total.aggregateByKey(0)((state, v) => state + 1, _+_).filter(data => data._2 >= y)
    result
  }
  
  def findPopular_Hubs (sc: SparkContext, x: Int, y: Int) = {
     val populars = findPopular(sc, x) // (state, num of passenger)
     val popular_flat = populars.flatMap(x => Seq(x._1, x._2._1, x._2._2))
     val hubs = findHubs(sc, y) // (state, num of flights)
     // result = (airport, (state num, incoming num, outgoing num))
     val result = hubs.join(populars_flat) // (state, (num of flights, in, out))
     
     result
}

}
