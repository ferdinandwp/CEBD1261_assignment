def factorial(n: Int): Int = { 
	var f = 1
	for(i <- 1 to n) 
	{ 
		f = f * i; 
	} 
	return f 
} 


val x = List(1,2,3,4,5,6,7,9,8)

//2a. use max to find maximum and compute factorial
val x_max = x.max
factorial(x_max)

//2b. use reduce method
val x_red = x.reduce((x, y) => x max y)
factorial(x_red)

//2c. Turn number into list and compute its factorial
val list_val = 1 to 7
val list_val_max = list_val.max
factorial(list_val_max)

