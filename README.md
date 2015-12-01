# MATS<sup>3</sup> - Message-based Asynchronous Transactional Staged Stateful Services

A library that facilitates the development of asynchronous multi-stage message-based services, where
a state object is maintained between each stage, and each stage is independently transactional.

The API consist nearly solely of interfaces, and can have implementations on several messaging platform,
but the current sole implementation is employing Java Message Service API - JMS v1.1.

# Rationale

In a multi-service architecture (e.g. <i>Micro Services</i>) one needs to communicate between the different services.
The golden hammer for such communication is REST services employing JSON over HTTP.

Asynchronous Message Oriented Middleware architectures are superior to synchronous REST-based systems in many ways, e.g.:

* High Availability (can have listeners on several servers for each queue)
* Scalability (can increase the number of listeners and servers for a queue, without any clients needing reconfiguration)
* Transactional-ability (each endpoint has either processed a message, done its DB-stuff, and sent a message, or none of it)
* Resiliency and Fault Tolerance (if a node goes down mid-way in processing, the transactional aspect kicks in and rolls back the processing, and another node picks up. Also, centralized retry.)
* Service Locating (only targets the logical queue name, without needing information about which servers are active for that endpoint)
* Debugging (the messages are typically strings and JSON, and can be inspected centrally on the Message Broker).
* Error handling (unprocessable messages will be refused and eventually put on a Dead Letter Queue, where they can be monitored, ops be alerted, and handled manually - centrally)

However, the big pain point with message-based communcations is that to reap all these benefits, one need to fully
embrace ansyncronous, multi-staged processing, where each stage is totally stateless, but where one still needs to
maintain a state throughout the flow. This is typically so hard to grasp, not to mention implement, that many
projects choose to go for the much easier model of synchronous processing where one can code linearly, employing
blocking calls out to other services that are needed - a model that every programmer intuitively know.

The main idea of this library is to let developers code message-based endpoints that themselves may "invoke"
other such endpoints, in a manner that closely resembles a synchronous "straight down" linear code style (envision
a plain Java method that invokes other methods, or more relevant, a REST service that invokes other REST services)
where state will be kept through the flow, but in fact ends up being a forward-only "Pipes and Filters" multi-stage
process. Effectively, the mental model feels like home, and coding is really straight-forward, while you reap all
the benefits of a Message Oriented Architecture.

# Examples

Some examples taken from the unit tests ("mats-lib-test"). Notice the use of the "MatsRule", which sets up an ActiveMQ in-memory server and creates a MatsFactory based on that.

In these examples, all the elements/endpoints are set up in one class, and thus obviously runs on the same machine - but in actual usage, you would typically have each
service run in a different project on different servers. The DTO-classes, which acts as the interface between the different endpoints' requests and replies, would then be copied between the projects (in this testing-scenario, one DTO class is used as all interfaces). 

## Simple send-receive

The following class tests the simplest functionality: Sets up a Terminator endpoint, and then an initiator sends a message to that endpoint. *(This example does not demonstrate the state keeping)*

ASCII-artsy, it looks like this:
<pre>
[Initiator]   {sends to}
[Terminator]
</pre>

```java
public class Test_SimplestSendReceive extends AMatsTest {
    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.getTrace());
                    matsTestLatch.resolve(dto, sto);
                });
    }

    @Test
    public void doTest() throws InterruptedException {
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

Tests the simplest request functionality: A single-stage service is set up. A Terminator is set up. Then an initiator does a request to the service, setting replyTo(Terminator). *(This example demonstrates state keeping between initiator and terminator)*

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
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> new DataTO(dto.number * 2, dto.string + ":FromService"));
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.getTrace());
                    matsTestLatch.resolve(dto, sto);
                });

    }

    @Test
    public void doTest() throws InterruptedException {
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

Sets up a somewhat complex test scenario, testing the basic state keeping and request-reply passing. 

The main aspects of MATS are demonstrated here, notice in particular how the code of <code>setupMasterMultiStagedService()</code> looks - if you squint a little - like a linear "straight down" method with two "blocking" requests out to other services, with a return at the end.
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


```java
public class Test_ComplexMultiStage extends AMatsTest {
    @Before
    public void setupLeafService() {
        matsRule.getMatsFactory().single(SERVICE + ".Leaf", DataTO.class, DataTO.class,
                (context, dto) -> new DataTO(dto.number * 2, dto.string + ":FromLeafService"));
    }

    @Before
    public void setupMidMultiStagedService() {
        MatsEndpoint<StateTO, DataTO> ep = matsRule.getMatsFactory().staged(SERVICE + ".Mid", StateTO.class,
                DataTO.class);
        ep.stage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(0, 0), sto);
            sto.number1 = 10;
            sto.number2 = Math.PI;
            context.request(SERVICE + ".Leaf", dto);
        });
        ep.lastStage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(10, Math.PI), sto);
            return new DataTO(dto.number * 3, dto.string + ":FromMidService");
        });
    }

    @Before
    public void setupMasterMultiStagedService() {
        MatsEndpoint<StateTO, DataTO> ep = matsRule.getMatsFactory().staged(SERVICE, StateTO.class, DataTO.class);
        ep.stage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(0, 0), sto);
            sto.number1 = Integer.MAX_VALUE;
            sto.number2 = Math.E;
            context.request(SERVICE + ".Mid", dto);
        });
        ep.stage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(Integer.MAX_VALUE, Math.E), sto);
            sto.number1 = Integer.MIN_VALUE;
            sto.number2 = Math.E * 2;
            context.request(SERVICE + ".Leaf", dto);
        });
        ep.lastStage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(Integer.MIN_VALUE, Math.E * 2), sto);
            return new DataTO(dto.number * 5, dto.string + ":FromMasterService");
        });
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.getTrace());
                    matsTestLatch.resolve(dto, sto);
                });
    }

    @Test
    public void doTest() throws InterruptedException {
        StateTO sto = new StateTO(420, 420.024);
        DataTO dto = new DataTO(42, "TheAnswer");
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR)
                        .request(dto, sto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2 * 3 * 2 * 5, dto.string + ":FromLeafService" + ":FromMidService"
                + ":FromLeafService" + ":FromMasterService"), result.getData());
    }
}
```

# Try it out!

If you want to try this out in your project, I will support you!

-Endre, endre@stolsvik.com
