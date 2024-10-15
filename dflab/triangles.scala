// Put your import statements here

import org.apache.spark.sql.Row 
import org.apache.spark.sql.types._
  
object DFlab {

    //create a test rdd, put in the correct types and create a datafrane
    def getTestDataFrame(spark) = {
      List((1,2,3), (4,5,6)).toDF("a", "b", "c")
   }
    def testGraph(spark) = {
        val test_graph = List(
            ("A", "B"),
            ("B", "A"),
            ("A", "C"),
            ("B", "C"),
            ("A", "D"),
            ("C", "E"),
            ("D", "C"),
            ("C", "A"),
            ("C", "B"),
            ("D", "A"),
            ("C", "D"),
            ("E", "C")
            )
         val test_DF = test_graph.toDF("start", "end")
   }

    def test_triangles(Df: DataFrame) = {
        val flipped = Df.map(case(a, b) => (b,a))
        val combined = Lst.union(flipped).distinct
        val selfjoin = combined.join(combined)
        val cleaned = selfjoin.filter{case (mid, (start, end)) => start != end} // 
        
}

    def triangleCOunt(Df: DataFrame): DataFrame = {
    
    val DFSchema = StructType(StructField("start", LongType, true), StructField("end", LongType, true))
    
    val fbDF = spark.read.format("csv").schema(DFSchema).option("header", true).load(/datasets/facebook)
    
    val fbDF_flipped = fbDF.withColumn("start", lit("end")).withColumn("end", lit("start"))
    val DF_combined = fbDF.union(fdDF_flipped).distinct
    val selfjoin = DF_combined.select("*", "(DF_combined.col("end") == DF_combined.col("start")) as mid")
    //| start| mid| end|
    val tri = self.where(col("start") = col("end"))

    }

}
~
