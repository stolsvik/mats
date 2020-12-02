# Mats<sup>3</sup> - Message-based Asynchronous Transactional Staged Stateless Services

[![Build Status](https://travis-ci.org/stolsvik/mats.svg?branch=master)](https://travis-ci.org/stolsvik/mats)

*Introducing "MOARPC" - Message-Oriented Asynchronous Remote Procedure Calls!*

*Mats* is a library that facilitates the development of asynchronous, stateless, multi-stage, message-based services. A Mats service can for example send a Request message to another Mats service, which processes the Request and sends a Reply message back to the next stage of the first service, after possibly itself having done one or several Requests/Replies to other services - with arbitrary nesting levels possible. Mats services consisting of multiple stages are connected with a *state object* which is passed along with the message flow, which gives an effect of having "request scoped variables" that are present through the different stages of the service. The intention is that coding such a Mats endpoint *feels like* coding a normal service method that performs synchronous RPC (e.g. REST) calls to other services, while reaping all the benefits of an asynchronous Message-Oriented communication paradigm. In addition, each stage is independently transactional, making a system made up of these services exceptionally resilient against any type of failure.

The API consist nearly solely of interfaces, not depending on any specific messaging platform's protocol or API. It can have implementations on several messaging platform, but the current sole implementation is employing Java Message Service API - JMS v1.1.

# Rationale

In a multi-service architecture (e.g. <i>Micro Services</i>) one needs to communicate between the different services. The golden hammer for such communication is REST services employing JSON over HTTP.

*(A note here: The arguments here are for service-mesh internal IPC/RPC communications. For e.g. REST endpoints facing the user/client, there is a "bridge" called the "MatsFuturizer", that can perform a Mats Request initiation which returns a Future, which will be resolved when the final Reply message comes back to the same node. There is also the library "MatsSockets" that is built on top of Mats that pulls the asynchronousness of Mats all the way out to the client.)*

However, *Asynchronous Message Oriented Middleware architectures* are superior to synchronous REST-based systems in many ways, for example:

* **High Availability**: For each queue, you can have listeners on several service instances on different physical servers, so that if one service instance or one server goes down, the others are still handling messages.
* **[Location Transparency](http://www.reactivemanifesto.org/glossary#Location-Transparency)**: *Service Location Discovery* is avoided, as messages only targets the logical queue name, without needing information about which nodes are currently consuming from that queue.
* **Scalability** / **Elasticity**: It is easy to [increase the number of nodes](http://www.reactivemanifesto.org/glossary#Replication) (or listeners per node) for a queue, thereby increasing throughput, without any clients needing reconfiguration. This can be done runtime, thus you get _elasticity_ where the cluster grows or shrinks based on the load, e.g. by checking the size of queues.
* **Handles load peaks** / **Prioritization**: Since there are queues between each part of the system and each stage of every process, a sudden influx of work (e.g. a batch process) will not deplete the thread pool of some service instance, bringing on cascading failures backwards in the process chain. Instead messages will simply backlog in a queue, being processed as fast as possible. At the same time, message prioritization will ensure that even though the system is running flat out with massive queues due to some batch running, any humans needing replies to queries will get in front of the queue. _Back-pressure_ (e.g. slowing down the entry-points) can easily be introduced if queues becomes too large.
* **Transactionality**: Each endpoint has either processed a message, done its work (possibly including changing something in a database), and sent a message, or none of it.
* **Resiliency** / **Fault Tolerance**: If a node goes down mid-way in processing, the transactional aspect kicks in and rolls back the processing, and another node picks up. Due to the automatic retry-mechanism you get in a message based system, you also get _fault tolerance_: If you get a temporary failure (database is restarted, network is reconfigured), or you get a transient error (e.g. a concurrency situation in the database), both the database change and the message reception is rolled back, and the message broker will retry the message.
* **Monitoring**: All messages pass by the Message Broker, and can be logged and recorded, and made statistics on, to whatever degree one wants.
* **Debugging**: The messages between different parts typically share a common format (e.g. strings and JSON), and can be inspected centrally on the Message Broker.
* **Error/Failure handling**: Unprocessable messages (both in face of coded-for conditions (validation), programming errors, and sudden external factors, e.g. db going down) will be refused and eventually put on a *Dead Letter Queue*, where they can be monitored, ops be alerted, and handled manually - centrally. Often, if the failure was some external resource being unavailable, a manual (or periodic attempts at) retry of the DLQ'ed message will restart the processing flow from the point it DLQ'ed, as if nothing happened.

However, the big pain point with message-based communications is that to reap all these benefits, one need to fully embrace asynchronous, multi-staged distributed processing, where each stage is totally stateless, but where one still needs to maintain a state throughout the flow.

## Standard Multi Service Architecture employing REST RPC 

When coding service-mesh internal REST endpoints, one often code locally within a single service, typically in a straight down, linear, often "transaction script" style - where you reason about the incoming Request, and need to provide a Response. It is not uncommon to do this in a synchronous fashion. This is very easy on the brain: It is a linear flow of logic. It is probably just a standard Java method. All the code for this particular service method resides in the same project. You can structure your code, invoke service beans or similar. When you need information from external sources, you go get it: Either from a database, or from other services by invoking them using some HttpClient. *(Of course, there are things that can be optimized here, e.g. asynchronous processing instead of old-school blocking Servlet-style calls, invoking the external resources concurrently by using e.g. Futures or other asynchronous mechanisms etc - but that is not really the point here!)*. For a given service, all logic can be mostly be reasoned about locally, within a single codebase.

In particular, *state* is not a problem - it is not even a thought - since that is handled by the fact that the process starts, runs, and finishes on the same JVM - often within a single method (e.g. a Servlet invocation, or a Spring @RequestMapping). If you first need to get something from one service, then use that information to get something from another service, and finally collate the information you have gathered into a response, you simply just do that: The implicit state between the stages of the process is naturally handled by local variables in the method, along with any incoming parameters of the method which also just naturally sticks around until the service returns the response. *(Unless you start with the mentioned asynchronous optimizations, which typically complicate such matters - but nevertheless: State is handled by the fact that this process is running within a single JVM)*.

However, the result is brittle: So, lets say your service invokes a second service. And then that second service again needs to invoke a third service. You now have three services that all have state on their respective stacks (either blocking a thread, or at any rate, having an asynchronous representation of the state on all three services). And, there is multiple stateful TCP connections: From the initial requester, to the receiving service, then to the second service, and then to the third service. The process is not finished until all services have provided their response, and thus the initial requester finally gets its response. If one service intermittently becomes slow, you face the situation of resource starvation with subsequent cascading failures percolating backwards in your call chains. If any of the service nodes fail, this will typically impact many ongoing processes, all of them failing, and the processes potentially being left in an intermediate state. Blindly doing retries in the face of such errors is dangerous too: You may not know whether the receiver actually processed your initial request or not: Did the error occur before the request was processed, while it was processed, or was it a problem with the network while the response was sent back? Even deploys of new versions can become a hazard, as you must think about these matters, and thus carefully perform graceful shutdowns letting all pending requests finish.

## Message-Oriented Multi Service Architecture

With a Messaging-Oriented Architecture, each stage of the total process would actually be a separate process. Instead of this nested call based logic (which is easy on the brain!), you now get a bunch of stages that the process flow needs to pass through: The process initiator puts a message on a queue, and another processor picks that up (probably on a different service / code base) - does some processing, and puts its (intermediate) result on another queue. This process may need to happen multiple times, traversing multiple services. This multiple *"post message - consume by another processor - process - post new message"* processing flow is harder to reason about. State is now not implicit, but needs to be passed along with the messages. And these stages will typically be within separate code bases, as they reside in different services.

You gain much: Each of these stages are independent processes. There is no longer a distributed blocking state residing through multiple services, as the queues acts as intermediaries, each stage processor having fully performed its part of the total process before posting the intermediate result on a queue (and then goes back fetching a new message to process). Each service can fire up a specific number of processors utilizing the available resources optimally. There cannot be a cascading failure scenario, at least not until the message queue broker is exhausted of space to store messages - which has a considerably higher ceiling than the thread and memory resources on individual processing nodes. The operations of consuming and posting messages are transactional operations, and if you don't mess too much up, each stage is independently transactional: If a failure occurs on a node, all processing having occurred on that node which have not run to completion will be rolled back, and simply retried on another node: The total process does not fail.

However, you also loose much: The simple service method where you could reason locally and code in a straight down manner, with state implicitly being handled by the ordinary machinery of the thread stack and local variables, now becomes a distributed process spread over several code bases, and any state needed between the process stages must now be handled explicitly. If you realize that you in the last stage need some extra information that was available in the initial stage, you must wade through multiple code bases to make sure this piece of information is forwarded through those intermediate stages. And let's just hope that you don't want to refactor the entire total process.

Thus, the overall structure in a message oriented architecture typically ends up being hard to grasp, quickly becomes unwieldy, and is difficult to implement due to the necessary multiple code bases that needs to be touched to implement/improve/debug a flow. Most projects therefore choose to go for the much easier model of synchronous processing using some kind of blocking REST-like RPC: In this model one can code linearly, employing blocking calls out to other, often *general* services that are needed - a model which every programmer intuitively know due to it closely mimicking local method invocations. However, the familiarity is basically the only positive aspect of this code style.

## What MATS brings to the table

**The main idea of the Mats library is to let developers code message-based endpoints that themselves may "invoke" other such endpoints, in a manner that closely resembles the familiar synchronous "straight down" linear code style.** In this familiar code style, all the state built up before the call out to the service is present after the service returns: Envision a plain Java method that invokes other methods, or more relevant, a REST service which invokes other REST services. With Mats, such an endpoint, while *looking* like a "straight down" synchronous method, is actually a multi-stage *(each stage processed by consumers on a specific queue)*, forward-only *(the flow passes from queue to queue, never synchronously waiting for a reply)*, fully stateless *(as state resides on the wire)*, asynchronous *(due to queueing)* message oriented distributed process.

**Effectively, the Mats library gives you a mental coding model which feels like home, and coding is really straightforward (both literally and figuratively!), while you reap all the benefits of a Message-Oriented Architecture.**

# Examples

Some examples taken from the unit tests ("mats-lib-test"). Notice the use of the JUnit Rule *Rule_Mats*, which sets up an ActiveMQ in-memory server and creates a JMS-backed MatsFactory based on that.

In these examples, all the endpoints and stages are set up in one test class, and thus obviously runs on the same machine - but in actual usage, you would typically have each endpoint run in a different process on different nodes. The DTO-classes, which acts as the interface between the different endpoints' requests and replies, would in real usage be copied between the projects (in this testing-scenario, one DTO class is used as all interfaces).

It is important to realize that each of the stages - also each stage in multi-stage endpoints - are handled by separate threads, and the state, request and reply objects are not shared references within a JVM - they are marshalled with the message that are passed between each stage of the services. *All information for a process resides as contents of the Mats envelope and data ("MatsTrace") which is being passed with the message.* In particular, if you have a process running a three-stage Mats endpoint which is running on two nodes A and B, for a particular request, the first stage might execute on node A, while the next stage on node B, and the last stage on node A again.

This means that what *looks like* a blocking, synchronous request/reply "method call" is actually fully asynchronous, where a reply from an invoked service will be handled in the next stage by a different thread, quite possibly on a different node if you have deployed the service in multiple instances.

## Simple send-receive

The following class exercises the simplest functionality: Sets up a Terminator endpoint, and then an initiator sends a message to that endpoint. *(This example does not demonstrate neither the stack (request/reply), nor state keeping - it is just the simplest possible message passing, sending some information from an initiator to a terminator endpoint)*

ASCII-artsy, it looks like this:
<pre>
[Initiator]   {sends to}
[Terminator]
</pre>

```java
public class Test_SimplestSendReceive extends AMatsTest {
    @Before
    public void setupTerminator() {
        // A "Terminator" is a service which does not reply, i.e. it "consumes" any incoming messages.
        // However, in this test, it countdowns the test-latch, so that the main test thread can assert.
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.getTrace());
                    matsTestLatch.resolve(dto, sto);
                });
    }

    @Test
    public void doTest() throws InterruptedException {
        // Send message directly to the "Terminator" endpoint.
        DataTO dto = new DataTO(42, "TheAnswer");
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(TERMINATOR)
                        .send(dto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(dto, result.getData());
    }
}
```

## Simple request to single stage "leaf-service"

Exercises the simplest request functionality: A single-stage service is set up. A Terminator is set up. Then an initiator does a request to the service, setting replyTo(Terminator). *(This example demonstrates one stack level request/reply, and state keeping between initiator and terminator)*

ASCII-artsy, it looks like this:
<pre>
[Initiator]   {request}
  [Service]   {reply}
[Terminator]
</pre>

```java
public class Test_SimplestServiceRequest extends AMatsTest {
    @Before
    public void setupService() {
        // This service is very simple, where it simply returns with an alteration of what it gets input.
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> {
                    return new DataTO(dto.number * 2, dto.string + ":FromService");
                });
    }

    @Before
    public void setupTerminator() {
        // A "Terminator" is a service which does not reply, i.e. it "consumes" any incoming messages.
        // However, in this test, it countdowns the test-latch, so that the main test thread can assert.
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.getTrace());
                    matsTestLatch.resolve(dto, sto);
                });

    }

    @Test
    public void doTest() throws InterruptedException {
        // Send request to "Service", specifying reply to "Terminator".
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR)
                        .request(dto, sto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getData());
    }
}
```

## Multi-stage service and multi-level requests

Sets up a somewhat complex test scenario, testing request/reply message passing and state keeping between stages in several multi-stage endpoints, at different levels in the stack.

The main aspects of Mats are demonstrated here, notice in particular how the code of <code>setupMasterMultiStagedService()</code> *looks like* - if you squint a little - a linear "straight down" method with two "blocking requests" out to other services, where the last stage ends with a return statement, sending off the reply to whoever invoked it.
<p>
Sets up these services:
<ul>
<li>Leaf service: Single stage: Replies directly.
<li>Mid service: Two stages: Requests "Leaf" service, then replies.
<li>Master service: Three stages: First requests "Mid" service, then requests "Leaf" service, then replies.
</ul>
A Terminator is also set up, and then the initiator sends a request to "Master", setting replyTo(Terminator).
<p>
ASCII-artsy, it looks like this:
<pre>
[Initiator]              {request}
    [Master S0 (init)]   {request}
        [Mid S0 (init)]  {request}
            [Leaf]       {reply}
        [Mid S1 (last)]  {reply}
    [Master S1]          {request}
        [Leaf]           {reply}
    [Master S2 (last)]   {reply}
[Terminator]
</pre>

**Again, it is important to realize that the three stages of the Master service (and the two of the Mid service) are actually fully independent messaging endpoints (with their own JMS queue when run on a JMS backend), and if you've deployed the service to multiple nodes, each stage in a particular invocation flow might run on a different node.**

The Mats API and implementation sets up a call stack that can be of arbitrary depth, along with "stack frames" whose state flows along with the message passing, so that you can code *as if* you were coding a normal service method that invokes remote services synchronously.


```java
public class Test_ComplexMultiStage extends AMatsTest {
    @Before
    public void setupLeafService() {
        // This service is very simple, where it simply returns with an alteration of what it gets input.
        matsRule.getMatsFactory().single(SERVICE + ".Leaf", DataTO.class, DataTO.class,
                (context, dto) -> {
                    return new DataTO(dto.number * 2, dto.string + ":FromLeafService");
                });
    }

    @Before
    public void setupMidMultiStagedService() {
        MatsEndpoint<StateTO, DataTO> ep = matsRule.getMatsFactory().staged(SERVICE + ".Mid",
                                                                            StateTO.class, DataTO.class);
        ep.stage(DataTO.class, (context, dto, sto) -> {
            // State object is "empty" at initial stage.
            Assert.assertEquals(0, sto.number1);
            Assert.assertEquals(0, sto.number2);
            // Setting state some variables.
            sto.number1 = 10;
            sto.number2 = Math.PI;
            // Perform request to "Leaf Service".
            context.request(SERVICE + ".Leaf", dto);                //  These three lines constitute the ..
        });                                                         //  .. "service call" from "Mid Service" ..
        ep.lastStage(DataTO.class, (context, dto, sto) -> {         //  .. out to the "Leaf Service" ..
            // .. "continuing" after the "Leaf Service" has replied.
            // Assert that state variables set in previous stage are still with us.
            Assert.assertEquals(10,      sto.number1);
            Assert.assertEquals(Math.PI, sto.number2);
            // Replying to calling service.
            return new DataTO(dto.number * 3, dto.string + ":FromMidService");
        });
    }

    @Before
    public void setupMasterMultiStagedService() {
        MatsEndpoint<StateTO, DataTO> ep = matsRule.getMatsFactory().staged(SERVICE,
                                                                            StateTO.class, DataTO.class);
        ep.stage(DataTO.class, (context, dto, sto) -> {
            // State object is "empty" at initial stage.
            Assert.assertEquals(0, sto.number1);
            Assert.assertEquals(0, sto.number2);
            // Setting some state variables.
            sto.number1 = Integer.MAX_VALUE;
            sto.number2 = Math.E;
            // Perform request to "Mid Service".
            context.request(SERVICE + ".Mid", dto);                 //  These three lines constitute the ..
        });                                                         //  .. "service call" from "Master Service" ..
        ep.stage(DataTO.class, (context, dto, sto) -> {             //  .. out to the "Mid Service" ..
            // .. "continuing" after the "Mid Service" has replied.
            // Assert that state variables set in previous stage are still with us.
            Assert.assertEquals(Integer.MAX_VALUE, sto.number1);
            Assert.assertEquals(Math.E,            sto.number2);
            // Changing the state variables.
            sto.number1 = Integer.MIN_VALUE;
            sto.number2 = Math.E * 2;
            // Perform request to "Leaf Service".
            context.request(SERVICE + ".Leaf", dto);                //  These three lines constitute the ..
        });                                                         //  .. "service call" from "Master Service" ..
        ep.lastStage(DataTO.class, (context, dto, sto) -> {         //  .. out to the "Leaf Service" ..
            // .. "continuing" after the "Leaf Service" has replied.
            // Assert that state variables changed in previous stage are still with us.
            Assert.assertEquals(Integer.MIN_VALUE, sto.number1);
            Assert.assertEquals(Math.E * 2,        sto.number2);
            // Replying to calling service.
            return new DataTO(dto.number * 5, dto.string + ":FromMasterService");
        });
    }

    @Before
    public void setupTerminator() {
        // A "Terminator" is a service which does not reply, i.e. it "consumes" any incoming messages.
        // However, in this test, it countdowns the test-latch, so that the main test thread can assert.
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    // .. "continuing" after the request to "Master Service" from Initiator.
                    log.debug("TERMINATOR MatsTrace:\n" + context.getTrace());
                    matsTestLatch.resolve(dto, sto);
                });
    }

    @Test
    public void doTest() throws InterruptedException {
        // Send request to "Master Service", specifying reply to "Terminator".
        StateTO sto = new StateTO(420, 420.024);  // State object for "Terminator".
        DataTO dto = new DataTO(42, "TheAnswer"); // Request object to "Master Service".
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR)
                        .request(dto, sto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        // Asserting that the state object which the Terminator got is equal to the one we sent.
        Assert.assertEquals(sto, result.getState());
        // Asserting that the final result has passed through all the services' stages.
        Assert.assertEquals(new DataTO(dto.number * 2 * 3 * 2 * 5,
                                       dto.string + ":FromLeafService" + ":FromMidService"
                                                  + ":FromLeafService" + ":FromMasterService"),
                            result.getData());
    }
}
```

# Try it out!

If you want to try this out in your project, I will support you!

-Endre, endre@stolsvik.com
