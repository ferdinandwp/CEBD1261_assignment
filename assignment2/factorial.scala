// Create scala object
object GFG
{ 
    // Define factorial function  
    def factorial(n: Int): Int = { 
          
        var f = 1
        for(i <- 1 to n) 
        { 
            f = f * i; 
        } 
          
        return f 
    } 
  
    // Fucntion to calculate factorial; replace the input as required 
    def main(args: Array[String])  
    { 
        println(factorial(5)) 
    }
  
} 
