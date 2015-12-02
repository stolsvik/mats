# Handling Double Deliveries

Set up correctly, a JMS broker (e.g. ActiveMQ) will ensure that a message is delivered at least once ("at-least-once delivery guarantee"). However, this obviously implies that sometimes a message might be delivered *more* than once. This can happen both due to configured redeliveries due to some exception happening inside the processing, but also due to situations outside of the control of the MatsStage developer, i.e. hardware or network problems, or broker failures.

Most of the time this will be caught and both the state changing database operations and the JMS operations will be rolled back, before the message is redelivered. However, very few things are perfect, and there are tiny glitches in the timeline of JMS & MATS processing where if at that precise moment the MATS-running process goes down or the JMS broker experiences some trouble, the JMS broker will not get the jmsSession.commit(). This either due to the commit() never being sent before the process dies and connection goes down, or the JMS broker never receiving it because it catches fire. However, it could have run the SQL commit() already.

In addition to any local state changing operations (CUD-operations in a DB) which might potentially correctly be rolled back, external operations might have been executed in the middle of a MatsStage, right before the MATS-running process crashes.

## Code idempotent!

Due to the fact that one *can* get double deliveries of any nasty kind, one should generally code in an [idempotent](https://en.wikipedia.org/wiki/Idempotence) manner, in such a way that if the message is received twice (or more), even though it has been executed before, the outcome won't be too bad. This is obviously trivial for "get-style" operations: A request for some information from some table, where the service replies with the result, won't impact the world negatively even though it is delivered dozens of times.

For state-changing operations, it might not be as simple.

## Situations

It is worth noting that the situation where the database is committed while the JMS message is not, which results in the message being redelivered even though the database operations have already been performed, is a *very* unlikely situation. But it might still happen.

Another more problematic situation, as it is more likely to happen (yet still "hit the Lotto!" if it happens), is where the message processing involves some work that is not possible to put within the MATS transaction, e.g. some call to an external REST service that dictates your bank to "send money to this person". The problem is that even though the entire processing is rolled back, the money is already sent, and if the redelivery kicks in, it will be sent twice.

Yet another more mundane, but even more likely problem is if your code actually raises some unexpected exception after the "send money" external REST command is fired off. Maybe your code was tested thoroughly against scenario A and B, while this new situation C makes it throw. In this case, all transactional stuff will be rolled back (JMS and DB), and the ordinary JMS redelivery will kick in, which by default for ActiveMQ is 1 delivery attempt, and 5 redeliveries. So, your money will be sent 6 times without any trace in your database, before the message is finally put on the Dead Letter Queue.

## Solutions: Focus on catching, not fixing

The suggestions here focuses on *catching* the situation, making it possible for the system to "call in the humans" by sending the message on the DLQ without causing more damage. That way, some operator can look at the logs, the database tables, or the bank account statement, and decide whether the operation actually went through or not the first time. For some operations, it might be possible to do such introspection automatically - but do remember to code offensively, and since these problems effectively never will happen, you should not put too much effort into making a huge AI-system to handle them when they do. So, by adding a means to *catch* the situation, you should be able to sleep well.

### Requester shall make unique "operationId"

First: Always let the sender, or requester, create some unique Id for each operation. Do not let that be the responsibility of the service receiving the request, and then have to deliver the id back. (This also makes sense in the asynchronous world, as you should not code in a way that requires you to hang around for any replies. Instead code in a "fire and forget"-style). For the send money example, this would mean that the requester sends a message *"Send money to this guy X; The transferId is 896d0c45-9138-418e-b8bc-4e5bcbf853bd"*. This way, you can uniquely identify the operation, which is the first step to catching that you are about to perform the same operation again.

### If "add to database", use DB unique constraint on operationId

If the operation boils down to "add some stuff into this database table" (think some "add new user" or "store document" operation), then a simple way to ensure Double Delivery catching is to have a column with the unique request id, and make that column have a unique constraint. Either the operation goes through, or the operation has already been performed, and you will get an SQLException.

### If external, non-transactional operation, use state machine

If it is an actual external REST service call, then the solution is a bit more involved.

You will need to create a state machine that will catch the situation, and these status changes must be committed while inside the MatsStage processing - which is inside a transaction.

See, everything within a MatsStage is contained within a transactional demarcation. You need to break out of this transaction to record your status changes, so that they are *not* rolled back if the MatsStage ends up being rolled back.

A straight forward way to do this is to make an extra DataSource that is not a part of the Mats transaction manager, so that you can easily get a new SQL Connection which is outside of the Mats transaction. (This may be to the same database as the Connection within the Mats transaction, or to a different state-machine database - that makes no difference, as it shall only record one single flag per operationId)

You then perform the following steps (pseudo-codeish):

```java
// :: Catch Double Deliveries!
ensureOperationIsNotAlreadyInProgressOrFinished(requestDto.operationId, nonTransactionalSqlConnection)
       throws RefuseMessageException;

// :: Add row in state table that we are in progress, committed right away.
addStatusInProgressForOperation(requestDto.operationId, nonTransactionalSqlConnection);

// :: Perform the "Never Again!" operation.
// (Make sure that any transactionId that is returned from the bank is logged using whatever
//  logging system you have set up, so that you can recover it if DB ops are rolled back.)
sendMoney(requestDto, [whateverIsNeeded], [transactionalSqlConnection]);

// :: Set to finished.
// (Not strictly necessary, but benefit of knowing how many are in progress.)
setStatusFinishedForOperation(requestDto.operationId, nonTransactionalSqlConnection);
```

This might seem messy, but remember that the goal here is to ensure that we never perform the dangerous method more than once - and we're happier with it happening zero times, than two, as long as we are informed about the problem in both directions.

Notice that you *could* handle the "already performed" situation gracefully: If the first ensureOperation...() call realizes that the operation has already finished, then it could just jump over the rest. I would not advice that, though, as I'd rather have big warnings about double deliveries happening in my system, than trying to cover them up.

Word of advice regarding error handling: If your system is not set up to handle all DLQ-messages as "Critical error!! Wake up some humans NOW!!", then you should just step away from this way of development right away.
