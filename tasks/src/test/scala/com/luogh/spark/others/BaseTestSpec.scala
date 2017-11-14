package com.luogh.spark.others

import org.scalatest.FunSpec

/**
  * @author luogh 
  */
class BaseTestSpec extends FunSpec {

  case class Apple(id: Int)

  describe("测试==与===的区别") {
    it("测试 == ") {
      assert(Apple(1) == Apple(1))
    }

    it("测试 === ") {
      assert(Apple(1) !== Apple(1))
    }
  }

}
