// where 절에서 사용 할 연산자
object Op extends Enumeration {
  type Op = Value

  val EQ = Value("=")
  val NE = Value("!=")
  val LTGT = Value("<>")
  val LT = Value("<")
  val LE = Value("<=")
  val GT = Value(">")
  val GE = Value(">=")
}
import Op._

// SQL 의 'WHERE x op value' 절
// +op+ 는 =, !=, <>, <, <=, >, >= 등의 비교 연산자
case class WhereOp[T](columnName: String, op: Op, value: T)

// SQL 의 'WHERE x IN (a, b, c, ...) 절
case class WhereIn[T](columnName: String, val1: T, vals: T*)

val wheres = Seq(
  WhereIn("state", "IL", "CA", "VA"),
  WhereOp("state", EQ, "IL"),
  WhereOp("name", EQ, "Buck Trends"),
  WhereOp("age", GT, 29),
)

for (where <- wheres) {
  where match {
    case WhereIn(col, val1, vals @ _*) =>
      val valStr = (val1 +: vals).mkString(", ")
      println(s"WHERE $col IN ($valStr)")
    case WhereOp(col, op, value) => println(s"WHERE $col $op $value")
    case _ => println(s"ERROR: Unknown expression: $where")
  }
}