for {
  x <- Seq(1, 2, 2.7, "one", "two", Symbol("four"))
} {
  val str = x match {
    case 1 => "int 1"
    case i: Int => s"other int: $i"
    case d: Double => s"a double: $d"
    case "one" => "string one"
    case s: String => s"other string: $s"
    case unexpected => s"unexpected value: $unexpected"
  }
  println(str)
}