import org.apache.spark.sql.Row 
import org.apache.spark.sql.types._
  
object fbcountdf {
    def FBCountDf(Df: DataFrame): DataFrame = {
    
    val DFSchema = new StructType.add(StructType(StructField("left", LongType, true)).add(StructType("right", LongType, true))
    
    val fbDF = spark.read.format("csv").schema(DFSchema).option("header", true).load(/datasets/facebook)

}
