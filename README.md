# MATS<sup>3</sup> - Message-based Asynchronous Transactional Staged Stateful Services

[![Build Status](https://travis-ci.org/stolsvik/mats.svg?branch=master)](https://travis-ci.org/stolsvik/mats)

A library that facilitates the development of asynchronous, stateless, multi-stage, message-based services. A MATS service may do a request to another MATS service, which replies back, after possibly itself doing a request to a third service - with arbitrary nesting levels possible. Services consisting of multiple stages are connected with a *state object* which is passed along in the subsequent message flow, which simulates the "this" reference when compared to traditional synchronous programming. In addition, each stage is independently transactional, making a system made up of these services exceptionally resilient against any type of failure.

The API consist nearly solely of interfaces, not depending on any specific messaging platform's protocol or API. It can have implementations on several messaging platform, but the current sole implementation is employing Java Message Service API - JMS v1.1.

# Rationale

In a multi-service architecture (e.g. <i>Micro Services</i>) one needs to communicate between the different services. The golden hammer for such communication is REST services employing JSON over HTTP.

However, *Asynchronous Message Oriented Middleware architectures* are superior to synchronous REST-based systems in many ways, for example:

* **High Availability**: Can have listeners on several nodes (servers/containers) for each queue, so that if one goes down, the others are still processing.
* **Scalability** / **Elasticity**: Can [increase the number of nodes](http://www.reactivemanifesto.org/glossary#Replication) (or listeners per node) for a queue, thereby increasing throughput, without any clients needing reconfiguration.
* **Handles load peaks** / **Prioritization**: Since there are queues between each part of the system, a sudden influx of work (e.g. a batch process) will not deplete some thread pool, instead messages will simply backlog in a queue, being processed as fast as possible. At the same time, message prioritization will ensure that humans needing replies to queries will get in front of the queue. _Back-pressure_ (e.g. slowing down the entry-points) can be easily be introduced if queues becomes too large.
* **Transactionality**: Each endpoint has either processed a message, done its work (including talking to external resources like a database), and sent a message, or none of it.
* **Resiliency** / **Fault Tolerance**: If a node goes down mid-way in processing, the transactional aspect kicks in and rolls back the processing, and another node picks up. Also, centralized retry.
* **[Location Transparency](http://www.reactivemanifesto.org/glossary#Location-Transparency)**: *Service Location Discovery* is avoided, as messages only targets the logical queue name, without needing information about which nodes are currently consuming from that queue.
* **Monitoring**: All messages pass by the Message Broker, and can be logged and recorded, and made statistics on, to whatever degree one wants.
* **Debugging**: The messages between different parts typically share a common format (e.g. strings and JSON), and can be inspected centrally on the Message Broker.
* **Error handling**: Unprocessable messages (both in face of coded-for conditions, programming errors, and sudden external factors, e.g. db going down) will be refused and eventually put on a *Dead Letter Queue*, where they can be monitored, ops be alerted, and handled manually - centrally. Often, if the failure was some external resource being unavailable, a redelivery will restart the processing flow from the point it DLQ'ed, as if nothing happened.

However, the big pain point with message-based communications is that to reap all these benefits, one need to fully embrace asynchronous, multi-staged distributed processing, where each stage is totally stateless, but where one still needs to maintain a state throughout the flow.

Whereas a REST-endpoint often by nature is multi-purpose, this doesn't hold for the typical message passing process. Let's take a HTTP GET method on service X: If it is general in nature, e.g. "GetOrdersForCustomers", it can be employed from several different services, e.g. service A, which considers outstanding orders for a set of customers against inventory, and service B, which validates that a new order can be added for a given customer. However, service A will most probably have different state requirements (it keeps the inventory in memory (on stack) while getting the sum of the orders) from service B (which has the current order in memory while checking the existing orders). This doesn't constitute a problem in a synchronous REST-employing service paradigm, since the state of the A process and the B process literally sits on the stack of the their respective processing threads: The different code bases is doing thread-blocking synchronous calls out to the same service X, where the variables (state) that was present before the call obviously is present after the call returns. (_If_ it returns, that is - let's hope no servers fails, and that the database of service X is up, and that the service isn't overloaded with no threads left, and that the "cascading failures" scenario does not happen.)

On the other hand, in a message passing scenario, the "Service X" with its "GetOrdersForCustomers" semantic will probably exist in several different incarnations: One for the aggregation flow A, and another for the validation flow B. This is because the state needed to be built up - *and passed along the message flow* - is different in the two different scenarios. Therefore, the different X's need to accept different built-up states, and after they have done their effectively identical job, they need to send their different resulting outgoing messages to different "next stage" message queues. Each scenario must therefore have its own queue and own processor for what effectively and semantically is just the same GetOrdersForCustomers.

Thus, the overall structure in a message oriented architecture typically ends up being hard to grasp, quickly becomes unwieldy, and is difficult to implement due to the necessary multiple code bases that needs to be touched to implement/improve/debug a flow. Most projects therefore choose to go for the much easier model of synchronous processing using some kind of blocking REST-like RPC: In this model one can code linearly, employing blocking calls out to other, *general*, services that are needed - a model that every programmer intuitively know due to it closely mimicking local method invocations. However, the familiarity is basically the only positive aspect of this code style.

The main idea of the MATS library is to let developers code message-based endpoints that themselves may "invoke" other such endpoints, in a manner that closely resembles the familiar synchronous "straight down" linear code style. In this familiar code style, all the state built up before the call out to the service is present after the service returns: Envision a plain Java method that invokes other methods, or more relevant, a REST service that invokes other REST services. With MATS, such an endpoint, while looking "straight down", the result is a multi-stage (each stage is its own queue), forward-only (the flow passes from queue to queue, never synchronously waiting for a reply), fully stateless (as state resides on the wire), asynchronous message oriented distributed process.

**Effectively, the MATS library gives you a mental coding model that feels like home, and coding is really straightforward (both literally and figuratively!), while you reap all the benefits of a Message Oriented Architecture.**

# Examples

Some examples taken from the unit tests ("mats-lib-test"). Notice the use of the JUnit Rule *Rule_Mats*, which sets up an ActiveMQ in-memory server and creates a JMS-backed MatsFactory based on that.

In these examples, all the endpoints and stages are set up in one test class, and thus obviously runs on the same machine - but in actual usage, you would typically have each
endpoint run in a different process on different nodes. The DTO-classes, which acts as the interface between the different endpoints' requests and replies, would in real usage be copied between the projects (in this testing-scenario, one DTO class is used as all interfaces).

It is important to realize that each of the stages - even each stage in multi-stage endpoints - are handled by separate threads, *sharing no contextual JVM state whatsoever:* The state, request and reply objects are not shared between stages. *There is no state shared between any stages except of the contents of the MATS envelope and data ("MatsTrace") which is being passed with the message.* In particular, if you have a process running a three-stage MATS endpoint which is running on two nodes A and B, the first stage might execute on A, while the next on B, and the last one on A again.

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

The main aspects of MATS are demonstrated here, notice in particular how the code of <code>setupMasterMultiStagedService()</code> *looks like* - if you squint a little - a linear "straight down" method with two "blocking requests" out to other services, where the last stage ends with a return statement, sending off the reply to whoever invoked it.
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

The MATS API and implementation sets up a call stack that can be of arbitrary depth, along with "stack frames" whose state flows along with the message passing, so that you can code *as if* you were coding a normal service method that invokes remote services synchronously.


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
