//Generate list
val x = 1 to 45
//divisible by 4; use remainder
val div_by_4 = x.filter(_ % 4 == 0)
//sum of all remaining list
val tot = div_by_4.sum


//divisible by 3; use remainder
val div_by_3 = x.filter(_ % 3 == 0)
val less_than_20 = div_by_3.filter(_ < 20)
def sqr(less_than_20: Int) = less_than_20 * less_than_20
val sqr_val = (less_than_20).map(sqr)

val tot_sqr_val = sqr_val.sum
