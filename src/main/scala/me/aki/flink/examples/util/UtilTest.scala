package me.aki.flink.examples.util

import java.util.Date

object UtilTest {
  def main(args: Array[String]): Unit = {
    val now = TimeUtil.format(new Date().getTime)
    println(now)
  }
}
