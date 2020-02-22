
// Define function for factorial
def factorial(n: Int): Int = { 
	var f = 1
	for(i <- 1 to n) 
	{ 
		f = f * i; 
	} 
	return f 
} 

// Driver Code 
def main(args: Array[String]) 
{ 
	println(factorial(5)) 
} 

// For other function simply do following command:
// go to spark-shell and write following script:
// factorial(6)
// factorial(7)
// factorial(8)
// factorial(9)

