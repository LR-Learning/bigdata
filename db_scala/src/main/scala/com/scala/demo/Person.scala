package com.scala.demo

/**
 * 接口多继承案例
 * @author LR
 * @create 2021-05-16:14:06
 */
object Person {
  def main(args: Array[String]): Unit = {
    val p1 = new Friend("tom")
    val p2 = new Friend("jack")

    p1.sayHello(p2.name)
    p1.makeFriends(p2)
  }

}

trait HelloTrait{def sayHello(name: String)}
trait MakeFriendsTrait{def makeFriends(p: Friend)}

class Friend(val name: String) extends HelloTrait with MakeFriendsTrait{
  def sayHello(name: String): Unit = {
    println("Hello")
  }

  def makeFriends(p: Friend): Unit = {
    println("my name is" + name + ",you name is" + p.name)
  }
}