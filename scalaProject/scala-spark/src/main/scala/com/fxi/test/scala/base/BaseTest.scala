package com.fxi.test.scala.base

import java.util.UUID

import org.junit.Test

/**
  * Created by xifei on 16-4-22.
  */
class BaseTest {
  @Test
  def testBaseOperation(): Unit = {
    // A opt B  == (A).opt(B)
    require((3).+(4) == 3 + 4)
    println("a" toString())
    println("abc" substring (1) equals "abc".substring(1))

    def g() {
      "this String gets lost too"
    }
    g()
  }

  @Test
  def testMvOperation(): Unit = {
    println(1 << 2)
  }

  /**
    * The implicit
    * modifier in front of the method tells the compiler to apply it automatically in
    * a number of situations.
    */
  @Test
  def testImplicitConversions(): Unit = {

    implicit def stringToInt(s: String): Int = s.hashCode
    println("aa" / 10)
  }

  /**
    * for clauses yield body
    * The yield goes before the entire body
    */
  @Test
  def testYield(): Unit = {
    val list = Array(1, 2, 3, 4, 5)
    def oddList = for {l <- list if l % 2 == 0} yield l
    oddList.foreach(f => println(f))
  }

  /**
    * (x: Int) => x + 1
    * The => designates that this function converts the thing on the left (any integer
    * x ) to the thing on the right ( x + 1 ).
    * 函数映射
    */
  @Test
  def testFirstClass(): Unit = {
    //定义了一个函数,从 x 转换到 x+1
    val increase = (x: Int) => x + 1
    println(increase(19))

    val someNumbers = List(-11, -10, -5, 0, 5, 10)
    someNumbers.filter((x: Int) => x > 0).foreach(f => println(f))
    someNumbers.filter(_ > 0).foreach(f => println(f)) //Placeholder syntax
    val f = (_: Int) + (_: Int)
    f(5, 19)
    someNumbers.foreach(println _) //someNumbers.foreach(x => println(x))

  }

  /**
    * The function value (the object) that’s created at runtime from this function
    * literal is called a closure.
    */
  @Test
  def testClosure(): Unit = {
    def makeIncreaser(more: Int) = (x: Int) => x + more
    val inc1 = makeIncreaser(1)
    val inc9999 = makeIncreaser(9999)
    inc1(10)
    inc9999(10)
  }

  /**
    * curring ==> 函数柯理化
    * 变成两个函数相继调用
    */
  @Test
  def testCurrying() = {
    def plainOldSum(x: Int, y: Int) = x + y
    println(plainOldSum(1, 3))

    def curriedSum(x: Int)(y: Int) = x + y
    println(curriedSum(1)(3))

    //===>定义了一个函数first 函数体又是一个函数
    def first(x: Int) = (y: Int) => x + y
    println(first(1)(3)) //相当于调用两次函数,第一次first(1) 返回一个函数 在调用 (3)
  //单个括号的时候可以用 花括号
    println(first {
      1
    } {
      3
    })

    val second = curriedSum(1) _;
    println(second(3))

    //定义一个函数twice,参数是一个叫op 输入输出都是double的函数,和一个x的double, 函数体是 执行两次op操作
    def twice(op: Double => Double, x: Double) = op(op(x))
    println(twice(_ + 1, 5))
  }

  /**
    * control abstractions
    */
  @Test
  def testControlAbstraction() = {
    //一个参数的时候可以用 花括号 代替 括号
    println("Hello, world!")
    println {
      "Hello, world!"
    }
  }

  /**
    * 空参数列表
    * => 函数体
    */
  @Test
  def testByNameParameter() = {
    var assertionsEnabled = true
    def myAssert(predicate: () => Boolean) =
      if (assertionsEnabled && !predicate())
        throw new AssertionError
//    myAssert(()=>3>5)

    //==> “() => Boolean”   --> “=> Boolean”

    def byNameAssert(predicate: => Boolean) =
      if (assertionsEnabled && !predicate)
        throw new AssertionError

    // 和 直接定义参数区别
    // predicate: Boolean <== 定义一个变量, 调用方法前已经计算
    // predicate: => Boolean  <== 定义了一个函数,没有参数,计算的时候才计算
    def boolAssert(predicate: Boolean) =
      if (assertionsEnabled && !predicate)
        throw new AssertionError

    byNameAssert(10/0 ==0)
    boolAssert(10/10 == 0)
    println("")
  }

  @Test
  def testImplicitParam() = {
    implicit val s = "aaa";
    def f(a:Int)(implicit s:String): Unit = {
      println(s)
      println(a)
    }

    f(1)
  }

  @Test
  def testFolds() = {
    val list  = List(5,4,8,6,2)
    val a = (1 /:list){ (z,i) => println(z + i) ; z+i }
    println(a)

    val b = (list :\ 1){ (z,i) => println(z + i) ; z+i }
    println(b)
  }

  @Test
  def testBitOperation() = {
    val x = 3L;
    println(x.toBinaryString)
    println(x << 1  )

    println(Long.MaxValue.toBinaryString)
    println((~(Long.MaxValue << 7)).toBinaryString)

    val a = Integer.parseInt("10010100011", 2)
    val b = a.toBinaryString.toCharArray
    val c = b.slice(b.length - 6, b.length)
    c.foreach(print _)
    println("--")
    println(c.count(p => {
      p == ("1".charAt(0))
    }))

    def parseActivityDays(days: Long, between: Int): Int = {
      val charArray = days.toBinaryString.toCharArray
      val length = Math.min(between, charArray.length)
      charArray.slice(charArray.length - length, charArray.length).count(p => {
        p == ("1".charAt(0))
      })
    }
    println(parseActivityDays(a, 60))

    def parseArrayCnt(array: String, between: Int): Int = {
      val items = array.split("#").map(f => f.toInt)
      val length = Math.min(between, items.length)
      items.slice(items.length - length, items.length).sum
    }
    println( parseArrayCnt("1#2#3#4#5",2))
  }

  @Test
  def testSplit() = {
    val x = "aa"
    print(x.split("#").length)
  }


  @Test
  def testStringToArrayConvertion() = {
    val x = "aa"
    val y = "aa#bb#cc#dd#ee"

    def doConvertion(x: String): String = {
      val a = x.split("#").toBuffer
      a += "nn"
      if (a.length > 5) {
        a.slice(a.length - 5, a.length).mkString("#")
      } else {
        a.mkString("#")
      }
    }

    println(doConvertion(x))
    println(doConvertion(doConvertion(y)))
  }

  @Test
  def testParseCount() = {
    val a = Integer.parseInt("1111000000000000000000000000000", 2)

    def timeReduceFunction(alpha: Double , days:Int): Double ={
      return (1 + alpha * days*days)
    }
    def parseActivityDays(days: Long, alpha: Double): Double = {
      val charArray = days.toBinaryString.toCharArray.reverse
      var summaryCnt = 0.0;
      for(i <- 0 until charArray.length){
          println(s"$i  ${charArray(i)}")
          if(charArray(i) == ("1".charAt(0))){
            summaryCnt+=1.toDouble/timeReduceFunction(alpha,i)
            println(1.toDouble/timeReduceFunction(alpha,i))
          }
      }
      summaryCnt
    }

    def parseArrayCnt(array: String, alpha: Double): Double = {
      val items = array.split("#")
      var summaryCnt = 0.0;
      for(i <- 0 until items.length){
        summaryCnt+=items(i).toDouble/timeReduceFunction(alpha,i)
      }
      summaryCnt
    }

    println( parseActivityDays(a,0.03))

  }

  @Test
  def testVectorOperation() = {


  }

  @Test
  def testCollectionOperation() = {
//    val a ="2 5 1 8 3".split(" ").map(f => f.toInt  ).sorted
//    a.foreach(println _)
//   println(("2 5 1 8 3".split(" ").toList.contains("21")))
    val m = Array(("a",1),("b",2),("c",3),("d",4),("e",5))
    println(m.toMap.keySet.contains("c1"))
  }

  @Test
  def testUUID() = {
    println(UUID.randomUUID().toString)
  }

  @Test
  def testReplace() = {
    val a ="11#22#44#0"
    println(a.substring(0,a.lastIndexOf("#0")+1)+"1")
    println(String.valueOf(null))
  }
}
