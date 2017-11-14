package com.luogh.spark

/**
  * @author luogh 
  */
object Test {

  def main(args: Array[String]): Unit = {
    val t = 1
    t match {
      case 0 =>
        print(0)
      case 1 =>
        print(1)
      case 2 =>
        print(2)
      case 3 =>
        print(3)
      case 4 =>
        print(4)
      case 5 =>
        print(5)
      case _ =>
        sys.error(s"wrong execute type[${t}]")
        sys.exit(1)
    }
  }

}
