package com.azuriteworlds.dsync

import java.time.Instant
import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

import cats.effect.IO
import cats.implicits._
import doobie.hikari._
import doobie.imports._

case class DatabaseConnection(connectionString: String, username: String, password: String, useSSL: Boolean = false)

/** A distributed lock with low latency */
case class SqlLockService(dbConnection: DatabaseConnection, locksTable: String = "distributed_locks", debugSqlQueries: Boolean = false) {
  private type XA = Transactor[IO]

  private val schemaEvolutions: Seq[Update0] = List(
    // A Yarn application is uniquely identified in the Yarn resource manager by
    // a Yarn "cluster timestamp" and "application_id"
    (fr"""CREATE TABLE IF NOT EXISTS """ ++ Fragment.const(locksTable) ++ fr""" (
        lock_name                VARCHAR(1000),
        expiration_date          DATETIME NOT NULL,
        PRIMARY KEY (lock_name)
      ) ENGINE = INNODB;
    """).update
  )

  private val transactor: Option[XA] = Try[XA] {
    HikariTransactor.newHikariTransactor[IO](
      "com.mysql.cj.jdbc.Driver",
      s"${dbConnection.connectionString}?serverTimezone=UTC&useSSL=${dbConnection.useSSL}&allowMultiQueries=true&failOverReadOnly=false&rewriteBatchedStatements=true",
      dbConnection.username,
      dbConnection.password
    ).unsafeRunSync
  } match {
    case Success(xa) =>
      performSchemaUpdates(xa)
      Some(xa)

    case Failure(t) =>
      println(t.getMessage)
      None
  }

  private def performSchemaUpdates(xa: XA): Unit = {
    val schemaEvolutionsTable = Fragment.const(s"${locksTable}_schema_evolutions")
    val createSchemaEvolutionTable: Update0 =
      (fr"""CREATE TABLE IF NOT EXISTS """ ++ schemaEvolutionsTable ++ fr""" (
          schema_version  SMALLINT NOT NULL,
          schema_update   DATETIME NOT NULL,
          PRIMARY KEY     (schema_version)
        ) ENGINE = INNODB;
      """).update
    createSchemaEvolutionTable.run
      .transact(xa)
      .unsafeRunSync()

    val currentSchemaVersion: Int = {
      val query: Query0[Option[Int]] = (fr"""SELECT MAX(schema_version) FROM """ ++ schemaEvolutionsTable).query[Option[Int]]
      query.unique
        .map(x => x.getOrElse(0))
        .transact(xa)
        .unsafeRunSync()
    }

    schemaEvolutions.zipWithIndex
      .drop(currentSchemaVersion)
      .foreach { case (evolution, i) =>
        (evolution.run *>
          (fr"""INSERT INTO """ ++ schemaEvolutionsTable ++ fr""" (schema_version, schema_update)
           VALUES (${i + 1}, ${Instant.now()})
            """).update.run)
          .transact(xa)
          .unsafeRunSync()
      }
  }

  private def deleteNamedLockFromDB(xa: XA, lockName: String): Unit = Try {
    (fr"""DELETE FROM """ ++ Fragment.const(locksTable) ++ fr""" WHERE lock_name = ${lockName}""").update.run.transact(xa).unsafeRunSync()
  } match {
    case Success(_) =>
    case Failure(_) => deleteNamedLockFromDB(xa, lockName)
  }

  /**
    * Acquires a distributed lock using an underlying MySql database for a maximum duration of ttlInSeconds. In case the lock
    * is already acquired, the lock acquisition will be retried every retryRate.
    **/
  def withDistributedLock[T](lockName: String, ttlInSeconds: Long, retryRate: Duration)(block: => Future[T]): Future[T] = {
    require(ttlInSeconds > 0, "Negative or null time to live is not allowed.")
    require(lockName.length <= 1000)

    implicit val doobieLogHandler: LogHandler = if (debugSqlQueries) LogHandler.jdkLogHandler else LogHandler.nop

    Future(()).flatMap { _ =>
      transactor match {
        case Some(xa) =>
          Try {
            val expirationDate = Instant.now().plusSeconds(ttlInSeconds)
            (for {
              _ <- (fr"""DELETE FROM """ ++ Fragment.const(locksTable) ++ fr""" WHERE lock_name = ${lockName} AND expiration_date <= NOW();""").update.run
              insertionResult <- (fr"""INSERT INTO """ ++ Fragment.const(locksTable) ++ fr""" (lock_name,expiration_date) VALUES (${lockName},${expirationDate});""").update.run
            } yield insertionResult).transact(xa).unsafeRunSync()
          } match {
            case Success(_) =>
              println(s"Acquired distributed lock $lockName")
              block.andThen { case _ =>
                println(s"Releasing lock $lockName")
                deleteNamedLockFromDB(xa, lockName)
              }

            case Failure(t) =>
              // println(t.getMessage)
              SqlLockService.ExecuteAfter(retryRate) { withDistributedLock(lockName, ttlInSeconds, retryRate)(block) }
          }

        case None => Future.failed(new Exception("No usable database found"))
      }
    }
  }
}

object SqlLockService {
  private object ExecuteAfter {
    private val executorService = Executors.newScheduledThreadPool(1, (r: Runnable) => {
      val t = Executors.defaultThreadFactory.newThread(r)
      t.setDaemon(true)
      t
    })

    def apply[T](delay: Duration)
                (block: => Future[T])
                (implicit executionContext: ExecutionContext): Future[T] = delay match {
      case Duration.Zero => block

      case _ =>
        val p = Promise[Unit]()
        executorService.schedule(
          new Runnable {
            def run(): Unit = p.success(())
          },
          delay.toMillis,
          TimeUnit.MILLISECONDS
        )

        p.future.flatMap(_ => block)(executionContext)
    }
  }
}