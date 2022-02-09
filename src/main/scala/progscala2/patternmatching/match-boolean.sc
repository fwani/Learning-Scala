val bools = Seq(true, false)

for (bool <- bools) {
//  bool match {
//    case true => println("Got heads")
//    case false => println("Got tails")
//  }
  val which = if (bool) "heads" else "tails"
  println(s"Got $which")
}