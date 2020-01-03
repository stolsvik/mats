# Mats and Websockets

Basic transport requirements
* Both plain text JSON and binary compressed JSON
* Must handle many types of messages


Message contents, from client:
* Standard contents: TraceId (set from client + added randomness)
* WS-MATS Session Id (make a new for each boot of a user's applications, i.e. if phone App boots, it should use a new session, and for each browser tab that boots, make a new session)
* WS-MATS CorrelationId for message

Need this to route back replies:
1. To correct WebSocket server, the one the client session is connected to (that is, on server side)
1. To correct Request message from client (that is, on client side)


 

Requirements
* Must be able to get one WS request, fire off a heap of Mats Requests, and that all these requests are routed back to the WS. 
* Must be possible for server to include the client as a endpoint. For this, use "stash/unstash".


# Notes
* Mats WebSocket Session Id (MWSSID) is the unique key to where messages will be routed. 
* One WebSocket is tied to one MWSSID.
* The MWSSID is dished out by the server upon CONNECT
* When reconnecting (RECONNECT), the client supplies the MWSSID it got on the CONNECT. This can typically be done upto 24h after lost connection, whereby any queued-up messages will be delivered.
* TimeOuts. Where to handle?!


System level messages
"isAuthOk?" -> "authIsOk!"


Typical connection
WS Connect.
  Connected.
Auth, "isAuthOk?"


## TODO, IDEAS
// Local Sessions
* matsSocketServer.getSessions();
* matsSocketServer.getSessionsByMatsSessionId(...);
* matsSocketServer.getSessionByPrincipalId(...); // AuthPlugin needs to provide an "id"
  * session.getPrincipal()
  * Session.sendMessage(...)

// Remote and Local Session
* matsSocketServer.sendMessageToMatsSessionId(...);
* matsSocketServer.sendMessageToPrincipalId(...);

// Adapt both on "temp jump" and reply
* matsSocketEndpoint.addForwardAdapter
  * hmm.. Maybe not necessary. It should be the "tempJump" that does the adaptation, while the store-and-forwarder just stores the finished adapted message.
  * .. then again, this requires that we need to re-establish the Principal.

// Statistics
* One minute into a session, the client sends the timings of all calls it has done since boot, w/timings.

// Error propagation
* Should be able to throw out of the adaptReply, and get this to the MatsSocket.js side

// System introspection
* Topic on which to query for all active Sessions, with a "replyToQueue" for where to send back info
  * Contains nodename, timestamp, and an object for each Session, with hos,an,av and "last msg ts"

// Queueing on Client side.
* Getting explicit "got your message" reply, removing from internal queue.
* Will try to resend if fails.

// Other types of authentication:
* How to handle HttpSession-style auth?
* How to handle Servlet Container auth?
* Maybe for both: Possible for de-auth to send special message to current WebSocket connection and ask to reevaluate
   Auth (we do have the currently provided Authorization string as part of session). If fails: tells Client about this,
   which will possibly simply disconnect and reconnect, and then get the auth failed.
   

```java
@MatsSocket("Endpoint.id")
public void handleMessage(MatsSocketContext<Blabla> context) {
    ctx.getAuthorization()  "Bearer: 4289429"
    ctx.getPrincipal()
    ctx.getMessage()  // To evaluate it

    // To forward into the MQ fabric
    ctx.matsForward("MatsEndpoint.id") // Use this..
    ctx.getMessageTingeling().from.to.blabla // .. or this - do not set from and to.
    
    // If you want to reply early, without forwarding 
    ctx.reply(new ReplyDto())
}



```




## Types of messages

Supports pipelining: Send multiple messages as list.

TODO: Handle debug information 


### Client-to-server: CONNECT and CONNECT_EXPECT (both initial, and reconnects), establishing MWSSID
* System message
* Further messages can be pipelined in the same go - but read further for distinction between CONNECT and CONNECT_EXPECT.
* SessionId SHALL NOT be included when starting a new Session
* Note: CONNECT can be used both for new Session and reconnects.  
* SessionId SHALL be included if CONNECT_EXPECT
* No message
* Provides the way to give initial authentication, some meta-info, and get back a the current SessionId.
* Difference between CONNECT and CONNECT_EXPECT:
  * "EXPECT" refers to the client expecting an existing connection, i.e. that the supplied SessionId exists. If the expected SessionId was not present, any pipelined messages are NOT executed. A SESSION_NEW reply will still be sent, now with the new SessionId.
  * The rationale for this distinction is when the application believes it just slept a second and hence pipelines messages that e.g. executes an order, while e.g. 30 hours has passed and the world has moved on, you might want such types of messages to be dropped, catch that a SESSION_NEW was returned instead of the expected SESSION_RECONNECTED, and thus "reboot" the application to initial state and let the user send the order again.
```
[{
    type: "HELLO"
    subType: "EXPECT_EXISTING" upon a reconnect (i.e. if sessionId is present).
    traceId: "AppStart[userInitiated]2897fswh"
    sessionId: "428959fjfvf8eh83" // Included if reconnect, not included if new session
    appName: "MegaApp2020-iOS"
    appVersion: "2019-11-24j14-477abef3"
    correlationId: "4289nd28df324329"
    jwt_access_token: ....... // Auth is required, must be valid and within timeout
    debug: {
       .. json ..
       .. json ..
    }
}]
```

### Server-to-client: SESSION_NEW and SESSION_RECONNECT
* System message - reply from CONNECT/CONNECT_EXPECT message
* Always includes current SessionId for this Mats WebSocket.
* If SessionId was not included in CONNECT, it will always be a SESSION_NEW (with, obviously, a new SessionId).
* If SessionId was supplied, it will wither be a SESSION_RECONNECTED if the Session was present (SessionId same as sent in), or SESSION_NEW if the session was not present (thus new SessionId).
* If it was a CONNECT_EXPECT, and this is a SESSION_NEW, any pipelined messages that was included with the CONNECT_EXPECT was dropped (i.e. not run).
```
[{
    type: "WELCOME"
    subType: "NEW" or "RECONNECTED"
    traceId: "AppStart[os:ios][appV:2019-11-24j142897fswh"
    sessionId: "428959fjfvf8eh83" // Either the one sent in, or a new one.
    correlationId: "4289nd28df324329"
    debug: {
       .. json ..
       .. json ..
    }
}]
```

### Server-to-client: AUTH_FAILED
* System message - but always as a reply to some other message.
* If the authentication failed, you will get this response.
* Can come for any message that include authentication (as is e.g. mandatory in CONNECT and CONNECT_EXPECT)
* The expectation is that when you supply authentication information, it will be ok - there is no "auth ok" message: If you supplied authentication information and do not get this message, it means that auth was ok.
```
[{
    type: "AUTH_FAILED"
    authfail: "<some enum value relevant for the authorization mechanism>"
    description: "<explanation, possibly including stacktrace>"
    traceId: "AppStart[os:ios][appV:2019-11-24j142897fswh"
    correlationId: "4289nd28df324329"
    debug: {
       .. json ..
       .. json ..
    }
}]
```
### Client-to-Server: Send (NOT expecting reply)
* Do not need correlationId then.
```
[{
    type: "SEND"
    endpointId: "WSOrder.place"
    traceId: "Order.place[pid:489342][cartId:4212]mncje42ax"
    sessionId: "428959fjfvf8eh83"
    jwt_access_token: ....... // optional if needed to update.
    message: { 
       .. json ..
       .. json ..
    }
    debug: {
       .. json ..
       .. json ..
    }
}]
```

### Client-to-Server: Request (expecting reply)
```
[{
    type: "REQUEST"
    endpointId: "WSOrder.place"
    replyToId: "Kamele.elg"
    traceId: "Order.place[pid:489342][cartId:4212]mncje42ax"
    sessionId: "428959fjfvf8eh83"
    correlationId: "4289nd28df324329"
    jwt_access_token: ....... // optional if needed to update.
    message: { 
       .. json ..
       .. json ..
    }
    debug: {
       .. json ..
       .. json ..
    }
}]
```

### Server-to-Client: Reply to request
```
[{
    type: "REPLY"
    traceId: Order.place[pid:489342][cartId:4212]mncje42ax
    sessionId: 428959fjfvf8eh83
    correlationId: 4289nd28df324329
    message: { 
       .. json ..
       .. json ..
    }
}]

```

### Server-to-Client: REQUEST (expecting reply)
WILL NOT BE IMPLEMENTED IN FIRST ITERATION, but is awesome cool.


### Server-to-Client: Send (NOT expecting reply)
* Do not need correlationId then.
```
[{
    type: "SEND"
    endpointId: "NotifyUser.maintenanceImminent"
    traceId: "WolfReboot[rebootId:42532]jkcvwe93"
    sessionId: "428959fjfvf8eh83"
    message: { 
       .. json ..
       .. json ..
    }
    debug: {
       .. json ..
       .. json ..
    }
}]
```

### Server-to-Client: "Exception" (The processing of a message failed)
* Application error, or TimeOut
* Can happen on MatsSocket reception.
```
[{
    type: "EXCEPTION"
    exceptionType: "TimeOut"
    exceptionDescription: "<explanation of what went wrong. Possibly including StackTrace if user is authorized.>"
    correlationId: 4289nd28df324329
}]
```



### Server-to-Client: "Message Error" (The received message was malformed)
* System message - but always as a reply to some other message.
* E.g. missing required field, or non-parseable JSON, or any other message-level error condition.
* Entire message will be returned in "incomingMessage" field, BASE64 encoded.
```
[{
    type: "MSGERROR"
    errorDescription: "<explanation of what went wrong. Possibly including StackTrace if user is authorized.>"
    incomingMessage: "<BASE64 encoded entire incoming message>"
}]
```

