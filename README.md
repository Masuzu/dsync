# dsync
Distributed lock in Scala for applications without low latency requirements, backed by a SQL database. Suitable for users not wanting to commit to other stacks such as Consul or Zookeeper which also provide distributed locking.

# Example usage

The locks provided by dsync have a maximum time to live so that other clients are not locked out forever when a client
holding a lock crashes without releasing its lock. Clients waiting for a lock release can
define how often to retry the lock acquisition (non busy waiting).

```
val lockService = SqlLockService(DatabaseConnection("jdbc:mysql://127.0.0.1:3388/test", "Masuzu", "MasuzuPassword"), debugSqlQueries = false)
  val f1 = lockService.withDistributedLock[Unit]("foo", 10, Duration(1, TimeUnit.SECONDS)) {
    Future {
      println("Test")
      Thread.sleep(4000)
    }
  }

  // f2 will print test only after f1 completes
  val f2 = lockService.withDistributedLock[Unit]("foo", 10, Duration.Zero) {
    Future {
      println("Test")
      Thread.sleep(4000)
    }
  }

  Await.result(Future.sequence(Seq(f1, f2)), Duration.Inf)
```