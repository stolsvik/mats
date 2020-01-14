package com.stolsvik.mats.websocket;

import java.security.Principal;

import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.Session;
import javax.websocket.server.HandshakeRequest;
import javax.websocket.server.ServerContainer;
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
         * Invoked straight after the HTTP WebSocket handshake request is performed, in the
         * {@link Endpoint#onOpen(Session, EndpointConfig)} invocation from the WebSocket {@link ServerContainer}. If
         * this method throws anything, the WebSocket request will immediately be terminated by invocation of
         * {@link Session#close()} - the implementation of this method may also itself do such a close. If you do
         * anything crazy with the Session object which will interfere with the MatsSocket messages, that's on you. One
         * thing that could be of interest, is to {@link Session#setMaxIdleTimeout(long) increase the max idle timeout}
         * - which will have effect wrt. to the initial authentication message that is expected, as the timeout is set
         * to a quite small value (after auth, the timeout is increased).
         *
         * @param handshakeRequest
         */
        default void evaluateHandshakeRequest(HandshakeRequest handshakeRequest, Session session) {
            /* no-op */
        }

        /**
         * Invoked when the MatsSocket connects over WebSocket, and is first authenticated by the Authorization string
         * typically supplied via the initial "HELLO" message from the client.
         * <p />
         * We would ideally want to evaluate upon the initial HTTP WebSocket handshake request whether we want to talk
         * with this client. The best solution would be an
         * <a href="https://tools.ietf.org/html/rfc6750#section-2.1">Authorization header</a> on this initial HTTP
         * request. However, since it is not possible to add headers via the WebSocket API in web browsers, this does
         * not work. We could have used cookies to achieve somewhat of the same effect (setting it via
         * <code>document.cookie</code> before running <code>new WebSocket(..)</code>), but since it is possible that
         * you'd want to connect to a different domain for the WebSocket than the web page was served from, this is is
         * not ideal as a general solution. Also, it makes for problems with "localhost" when developing. Lastly, we
         * could have added it as a URI parameter, but this is
         * <a href="https://tools.ietf.org/html/rfc6750#section-2.3">strongly discouraged</a>.
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
         * <b>NOTE!</b> You might want to hold on to the 'authorizationHeader' between the invocations.
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

    /**
     * You are NOT supposed to implement this interface! Instances of this interface are created by methods on the
     * {@link AuthenticationContext}, which you are supposed to return from the {@link SessionAuthenticator} to inform
     * the {@link MatsSocketServer} about your verdict of the authentication attempt.
     */
    interface AuthenticationResult {
        /* nothing of your concern */
    }
}
