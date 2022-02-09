import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

def sleep(millis: Long): Unit = {
  Thread.sleep(millis)
}

def doWork(index: Int): Int = {
  sleep((math.random * 1000).toLong)
  index
}

(1 to 5) foreach { index =>
  val future = Future {
    doWork(index)
  }
  future foreach {
    case answer: Int => println(s"Success! returned: $answer")
    case unexpected => println(s"Failure! returned: $unexpected")
  }
}

sleep(1000)
println("Finish!")