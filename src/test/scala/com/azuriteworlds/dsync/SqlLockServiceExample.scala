package com.azuriteworlds.dsync

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

import com.wix.mysql.EmbeddedMysql
import com.wix.mysql.config.Charset.UTF8
import com.wix.mysql.config.MysqldConfig
import com.wix.mysql.distribution.Version.v5_7_latest

object SqlLockServiceExample {
  def main(args: Array[String]): Unit = {
    val mySqlConfig = MysqldConfig.aMysqldConfig(v5_7_latest)
      .withUser("Masuzu", "MasuzuPassword")
      .withCharset(UTF8)
      .withPort(3388)
      .withTimeout(3600, TimeUnit.SECONDS)
      .build()

    val mysqld = EmbeddedMysql.anEmbeddedMysql(mySqlConfig).addSchema("test").start()

    println("started!")
    println("if needed you can connect to this running db using:")
    println("> mysql -u root -h 127.0.0.1 -P 3388 test")
    println("press [Ctrl+D] to stop...")

    val lockService = SqlLockService(DatabaseConnection("jdbc:mysql://127.0.0.1:3388/test", "Masuzu", "MasuzuPassword"), debugSqlQueries = false)
    val f1 = lockService.withDistributedLock[Unit]("foo", 10, Duration(1, TimeUnit.SECONDS)) {
      Future {
        println("Test")
        Thread.sleep(4000)
      }
    }

    val f2 = lockService.withDistributedLock[Unit]("foo", 10, Duration.Zero) {
      Future {
        println("Test")
        Thread.sleep(4000)
      }
    }

    Await.result(Future.sequence(Seq(f1, f2)), Duration.Inf)

    mysqld.stop()
  }
}
