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

<i>[In the following I will refer to the Enterprise Integration Patters book by Gregor Hohpe and Bobby Woolf as "EIP".]</i>

TO BE CONTINUED..

