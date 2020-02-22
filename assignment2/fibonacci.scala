val fibs:Stream[Int] = 0 #:: 1 #:: (fibs zip fibs.tail).map{ t => t._1 + t._2 }
fibs take 10 toList
