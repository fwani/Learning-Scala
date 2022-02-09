// 직접 main 함수를 생성해서 실행하는 방법
object HelloWorldObject {
  def main(args: Array[String]): Unit = {
    println("Hello World main")
  }
}

// App trait 을 상속 받아서 main 을 사용하는 방법
object HelloWorld extends App{
  println("Hello World main")
  println(
    """a
      |c""".stripMargin)
}