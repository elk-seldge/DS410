case class Neumaier(sum: Double, c: Double)
import scala.math.abs

object HW {

   def q1_countsorted(x: Int, y: Int, z:Int):Int = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      val flg1 = y-x
      val flg2 = z-y
      val flg3 = z-x
      val rtn = if (flg1 > 0 || flg2>0 || flg3>0){ // one condition met
          1
      }else if ( (flg1>0 && flg2>0) || (flg1 >0 && flg3>0) || (flg2>0 && flg3>0) ){ // 2 cnd met
          2
      }else if (flg1>0 && flg2>0 && flg3>0 ){ // 3 cnd met
          3
      }else{      
      -1
      }
      rtn
   }

   def q2_interpolation(name: String, age: Int):String = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      val rtn = if (age>20){
      s"hello, $name".toLowerCase()
      }else{s"howdy, $name".toLowerCase()}
      rtn
   }

   def q3_polynomial(arr: Seq[Double]):Double = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      val rtn = arr.foldLeft(0.0){(y,x) => y + x * (arr.indexOf(x)+1)}
      rtn
   }

   def q4_application(x: Int, y: Int, z: Int)(f: (Int, Int) => Int):Int = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      f(f(x, y), z)  
   }

   def q5_stringy(start: Int, n: Int): Vector[String] = {
      val rtn = Vector.tabulate(n){x => (x+start).toString}
      rtn
   }

   def q6_modab(a: Int, b: Int, c: Vector[Int]): Vector[Int] = {
      val temp = c.filter{x => x >= a}
      val rtn = temp.filter{x => x%b != 0}
      rtn  
}

   def q7_count(arr: Vector[Int])(f: Int => Boolean):Int = { 
      // base case is the head element of every slice, if match, cnt +1
      val current = arr.head // first element
      val next = arr.tail // rest of element

      if (next.isEmpty) { // no more remained, base case
         if (f(current)){1} 
         else{0}
      }else{
         if (f(current)) {
            q7_count(next)(f) + 1
         }else{
            q7_count(next)(f)
         }
      }
   }

   @annotation.tailrec
   def q8_count_tail(arr: Vector[Int], count: Int=0)(f: Int => Boolean) : Int = {
      // nth after recursion
      val current = arr.head // first element
      val next = arr.tail // rest of element
      if (next.isEmpty) {
         if (f(current)){count + 1}
         else{count}
      }
      else{
         if (f(current)) {q8_count_tail(next,count + 1)(f)}
         else{q8_count_tail(next,count)(f)}
      }
   }

   def q9_neumaier(arr: Seq[Double]): Double = {
      val temp = arr.foldLeft(Neumaier(0.0, 0.0)){(y,x) => {
          val rec = y.sum + x
          if (abs(y.sum) >= abs(x)){
            Neumaier(rec, (y.sum-rec)+x)
          }else{
            Neumaier(rec, (x-rec)+y.sum)
          }
      }}
      temp.sum + temp.c
   }

   // create the rest of the functions yourself
   // in order for the code to compile, you need to (at the very least) create
   // blank versions of the remaining functions and have them return a value of 
   // the expected type, like the blank functions above.
   // remember, to compile, you don't specify any file names, you just use sbt compile
}
