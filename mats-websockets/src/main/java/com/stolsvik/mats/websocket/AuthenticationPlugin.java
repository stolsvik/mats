package com.stolsvik.mats.websocket;

import java.security.Principal;
import java.util.EnumSet;

import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.HandshakeResponse;
import javax.websocket.Session;
import javax.websocket.server.HandshakeRequest;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Configurator;

import com.stolsvik.mats.websocket.MatsSocketServer.IncomingAuthorizationAndAdapter;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpointRequestContext;

/**
 * Plugin that must evaluate whether a MatsSocket connection is authenticated. It receives the String that the client
 * provides, and evaluates whether it is good, and if so returns a Principal and a UserId.
 *
 * @author Endre Stølsvik 2020-01-10 - http://stolsvik.com/, endre@stolsvik.com
 */
@FunctionalInterface
public interface AuthenticationPlugin {

    /**
     * Invoked by the {@link MatsSocketServer} upon each WebSocket connection that wants to establish a MatsSocket -
     * that is, you may provide a connections-specific instance per connection. The server will then invoke the
     * different methods on the returned {@link SessionAuthenticator}.
     *
     * @return an instance of {@link SessionAuthenticator}, where the implementation may choose whether it is a
     *         singleton, or a new is returned per invocation (which then means there is a unique instance per
     *         connection, thus you can hold values for the connection there).
     */
    SessionAuthenticator newSessionAuthenticator();

    interface SessionAuthenticator {
        /**
         * Implement this if you want to do a check on the Origin header value while the initial WebSocket
         * Upgrade/Handshake request is still being processed. This invocation is directly forwarded from
         * {@link Configurator#checkOrigin(String)}. Note that you do not have anything else to go by here, it is a
         * static check. If you want to evaluate other headers (or probably more relevant for web clients, parameters),
         * to decide upon allowed Origins, you can get the Origin header from the {@link HandshakeRequest} within the
         * {@link #checkHandshake(ServerEndpointConfig, HandshakeRequest, HandshakeResponse)} checkHandshake(..)} - that
         * is however one step later in the establishing of the socket (response is already sent, so if you decide
         * against the session there, the socket will be opened, then immediately closed).
         * <p/>
         * The default implementation returns <code>true</code>, i.e. letting all connections through this step.
         *
         * @param originHeaderValue
         *            which Origin the client connects from - this is mandatory for web browsers to set.
         * @return whether this Origin shall be allowed to connect. Default implementation returns <code>true</code>.
         */
        default boolean checkOrigin(String originHeaderValue) {
            return true;
        }

        /**
         * Implement this if you want to do a check on any other header values, e.g. Authorization or Cookies, while the
         * initial WebSocket Upgrade/Handshake request is still being processed - before the response is sent. This
         * invocation is directly forwarded from
         * {@link Configurator#modifyHandshake(ServerEndpointConfig, HandshakeRequest, HandshakeResponse)}. You may add
         * headers to the response object. If this method returns <code>false</code> or throws anything, the WebSocket
         * request will immediately be terminated.
         * <p />
         * <b>NOTE!</b> We would ideally want to evaluate upon the initial HTTP WebSocket handshake request whether we
         * want to talk with this client. The best solution would be an
         * <a href="https://tools.ietf.org/html/rfc6750#section-2.1">Authorization header</a> on this initial HTTP
         * request. However, since it is not possible to add headers via the WebSocket API in web browsers, this does
         * not work. Secondly, we could have added it as a URI parameter, but this is
         * <a href="https://tools.ietf.org/html/rfc6750#section-2.3">strongly discouraged</a>. We can use cookies to
         * achieve somewhat of the same effect (setting it via <code>document.cookie</code> before running
         * <code>new WebSocket(..)</code>), but since it is possible that you'd want to connect to a different domain
         * for the WebSocket than the web page was served from, this is is not ideal as a general solution. A trick is
         * implemented in MatsSocket, whereby it can do a XmlHttpRequest to a HTTP service that is running on the same
         * domain as the WebSocket, which can move the Authorization header to a Cookie, which will be sent along with
         * the WebSocket Handshake request - and can thus be checked here.
         * <p />
         * The default implementation return <code>true</code>, i.e. letting all connections through this step.
         *
         * @param config
         *            the {@link ServerEndpointConfig} instance.
         * @param request
         *            the HTTP Handshake Request
         * @param response
         *            the HTTP Handshake Response, upon which it is possible to set headers.
         * @return <code>true</code> if the connection should be let through, <code>false</code> if not.
         */
        default boolean checkHandshake(ServerEndpointConfig config, HandshakeRequest request,
                HandshakeResponse response) {
            return true;
        }

        /**
         * Invoked straight after the HTTP WebSocket handshake request/response is performed, in the
         * {@link Endpoint#onOpen(Session, EndpointConfig)} invocation from the WebSocket {@link ServerContainer}. If
         * this method returns <code>false</code> or throws anything, the WebSocket request will immediately be
         * terminated by invocation of {@link Session#close()} - the implementation of this method may also itself do
         * such a close. If you do anything crazy with the Session object which will interfere with the MatsSocket
         * messages, that's on you. One thing that could be of interest, is to {@link Session#setMaxIdleTimeout(long)
         * increase the max idle timeout} - which will have effect wrt. to the initial authentication message that is
         * expected, as the timeout setting before authenticated session is set to a quite small value by default: 2.5
         * seconds (after auth, the timeout is increased). This short timeout is meant to make it slightly more
         * difficult to perform DoS attacks by opening many connections and then not send the initial auth-containing
         * message. The same goes for {@link Session#setMaxTextMessageBufferSize(int) max text message buffer size}, as
         * that is also set low until authenticated: 20KiB.
         * <p />
         * The default implementation return <code>true</code>, i.e. letting all connections through this step.
         *
         * @param webSocketSession
         *            the WebSocket API {@link Session} instance.
         * @param config
         *            the {@link ServerEndpointConfig} instance.
         * @return <code>true</code> if the connection should be let through, <code>false</code> if not.
         */
        default boolean onOpen(Session webSocketSession, ServerEndpointConfig config) {
            return true;
        }

        /**
         * Invoked when the MatsSocket connects over WebSocket, and is first authenticated by the Authorization string
         * typically supplied via the initial "HELLO" message from the client.
         * <p />
         * <b>NOTE!</b> Read the JavaDoc for
         * {@link #checkHandshake(ServerEndpointConfig, HandshakeRequest, HandshakeResponse) checkHandshake(..)} wrt.
         * evaluating the actual HTTP Handshake Request, as you might want to do auth already there.
         *
         * @param context
         *            were you may get additional information (the {@link HandshakeRequest} and the WebSocket
         *            {@link Session}), and can create {@link AuthenticationResult} to return.
         * @param authorizationHeader
         *            the string value to evaluate for being a valid authorization, in any way you fanzy - it is
         *            supplied by the client, where you also will have to supply a authentication plugin that creates
         *            the strings that you here will evaluate.
         * @return an {@link AuthenticationResult}, which you get from any of the method on the
         *         {@link AuthenticationContext}.
         */
        AuthenticationResult initialAuthentication(AuthenticationContext context, String authorizationHeader);

        /**
         * Invoked on every subsequent "pipeline" of messages (including every time the client supplies a new
         * Authorization string). This should thus be as fast as possible, preferably using only code without doing any
         * IO. If the 'existingPrincipal' is still valid (and with that also the userId that was supplied at
         * {@link AuthenticationContext#authenticated(Principal, String)}), then you can invoke
         * {@link AuthenticationContext#stillValid()}.
         * <p />
         * <b>NOTE!</b> You might want to hold on to the 'authorizationHeader' between the invocations to quickly
         * evaluate whether it has changed: In e.g. an OAuth setting, where the authorizationHeader is a bearer access
         * token, you could shortcut evaluation: If it has not changed, then you might be able to just evaluate whether
         * it has expired by comparing a timestamp that you stored when first evaluating it, towards the current time.
         * If the authorizationHeader (token) has changed, you would do full evaluation of it.
         *
         * @param context
         *            were you may get additional information (the {@link HandshakeRequest} and the WebSocket
         *            {@link Session}), and can create {@link AuthenticationResult} to return.
         * @param authorizationHeader
         *            the string value to evaluate for being a valid authorization, in any way you fanzy - it is
         *            supplied by the client, where you also will have to supply a authentication plugin that creates
         *            the strings that you here will evaluate.
         * @param existingPrincipal
         *            The {@link Principal} that was returned with the last authentication (either initial or
         *            reevaluate).
         * @return an {@link AuthenticationResult}, which you get from any of the method on the
         *         {@link AuthenticationContext}.
         */
        AuthenticationResult reevaluateAuthentication(AuthenticationContext context, String authorizationHeader,
                Principal existingPrincipal);
    }

    interface AuthenticationContext {
        /**
         * @return the {@link HandshakeRequest} that was provided to the JSR 356 WebSocket API's Endpoint
         *         {@link Configurator Configurator} when the client connected. Do realize that there is only a single
         *         HTTP request involved in setting up the WebSocket connection: The initial "Upgrade: WebSocket"
         *         request.
         */
        HandshakeRequest getHandshakeRequest();

        /**
         * @return the WebSocket {@link Session} instance. You should not be too creative with this.
         */
        Session getWebSocketSession();

        /**
         * Return the result from this method from
         * {@link SessionAuthenticator#initialAuthentication(AuthenticationContext, String)
         * SessionAuthenticator.initialAuthentication(..)} or
         * {@link SessionAuthenticator#reevaluateAuthentication(AuthenticationContext, String, Principal)}
         * SessionAuthenticator.reevaluateAuthentication(..)} to denote BAD authentication, supplying a reason string
         * which will be sent all the way to the client (so do not include sensitive information).
         *
         * @param reason
         *            a String which will be sent all the way to the client (so do not include sensitive information).
         * @return an {@link AuthenticationResult} that can be returned by the methods of {@link SessionAuthenticator}.
         */
        AuthenticationResult notAuthenticated(String reason);

        /**
         * Return the result from this method from
         * {@link SessionAuthenticator#initialAuthentication(AuthenticationContext, String)
         * SessionAuthenticator.initialAuthentication(..)} to denote good authentication, supplying a Principal
         * representing the accessing user, and the UserId of this user. You can also return the result from this method
         * from {@link SessionAuthenticator#reevaluateAuthentication(AuthenticationContext, String, Principal)
         * SessionAuthenticator.reevaluateAuthentication(..)} if you want to change the Principal, typically just to
         * update some meta data values, as it would be strange if such reevaluation of authentication resulted in a
         * different user than last time.
         *
         * @param principal
         *            the Principal that will be supplied to all
         *            {@link IncomingAuthorizationAndAdapter#handleIncoming(MatsSocketEndpointRequestContext, Principal, Object)}
         *            calls, for the MatsSocket endpoints to evaluate for authorization or to get needed user specific
         *            data from (typically thus casting the Principal to a specific class for this
         *            {@link AuthenticationPlugin}).
         * @param userId
         *            the user id for the Principal - this is needed separately from the Principal so that it is
         *            possible to target a specific user via a send or request from server to client.
         * @return an {@link AuthenticationResult} that can be returned by the methods of {@link SessionAuthenticator}.
         */
        AuthenticationResult authenticated(Principal principal, String userId);

        /**
         * Variant of {@link #authenticated(Principal, String)} that grants the authenticated user special abilities to
         * ask for debug info of the performed call.
         *
         * @param principal
         *            the Principal that will be supplied to all
         *            {@link IncomingAuthorizationAndAdapter#handleIncoming(MatsSocketEndpointRequestContext, Principal, Object)}
         *            calls, for the MatsSocket endpoints to evaluate for authorization or to get needed user specific
         *            data from (typically thus casting the Principal to a specific class for this
         *            {@link AuthenticationPlugin}).
         * @param userId
         *            the user id for the Principal - this is needed separately from the Principal so that it is
         *            possible to target a specific user via a send or request from server to client.
         * @param allowedDebugOptions
         *            Which types of Debug stuff the user is allowed to ask for. The resulting debug options is the
         *            "logical AND" between these, and what the client requests.
         * @return an {@link AuthenticationResult} that can be returned by the methods of {@link SessionAuthenticator}.
         */
        AuthenticationResult authenticated(Principal principal, String userId,
                EnumSet<DebugOptions> allowedDebugOptions);

        /**
         * Return the result from this method from
         * {@link SessionAuthenticator#reevaluateAuthentication(AuthenticationContext, String, Principal)} if the
         * 'existingPrincipal' still is good to go.
         *
         * @return an {@link AuthenticationResult} that can be returned by the method
         *         {@link SessionAuthenticator#reevaluateAuthentication(AuthenticationContext, String, Principal)},
         *         stating that the existing authorization is still valid.
         */
        AuthenticationResult stillValid();
    }

    enum DebugOptions {
        /**
         * Timing info of the separate phases. Note that time-skewing between hosts must be taken into account.
         */
        TIMINGS(0b1),

        /**
         * Node-name of the handling node of the separate phases.
         */
        NODES(0b01);

        final int bitconstant;

        DebugOptions(int bitconstant) {
            this.bitconstant = bitconstant;
        }
    }

    /**
     * You are NOT supposed to implement this interface! Instances of this interface are created by methods on the
     * {@link AuthenticationContext}, which you are supposed to return from the {@link SessionAuthenticator} to inform
     * the {@link MatsSocketServer} about your verdict of the authentication attempt.
     */
    interface AuthenticationResult {
        /* nothing of your concern */
    }
}
