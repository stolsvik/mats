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

import com.stolsvik.mats.websocket.MatsSocketServer.ActiveMatsSocketSession;
import com.stolsvik.mats.websocket.MatsSocketServer.IncomingAuthorizationAndAdapter;
import com.stolsvik.mats.websocket.MatsSocketServer.LiveMatsSocketSession;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketCloseCodes;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpointIncomingContext;

/**
 * Plugin that must evaluate whether a WebSocket connection shall be allowed, and then authenticate the resulting
 * MatsSocketSession.
 * <ol>
 * <li>Upon WebSocket connection, the methods {@link SessionAuthenticator#checkOrigin(String) checkOrigin(..)},
 * {@link SessionAuthenticator#checkHandshake(ServerEndpointConfig, HandshakeRequest, HandshakeResponse)
 * checkHandshake(..)} and {@link SessionAuthenticator#onOpen(Session, ServerEndpointConfig) onOpen(..)} are invoked in
 * succession.</li>
 * <li>During the initial set of messages received from the MatsSocket Client, the
 * {@link SessionAuthenticator#initialAuthentication(AuthenticationContext, String) initialAuthentication(..)} is
 * invoked, which evaluates whether the supplied Authorization "Header" value is good, and if so returns a Principal and
 * a UserId.</li>
 * <li>During the life of the MatsSocketSession, the two methods
 * {@link SessionAuthenticator#reevaluateAuthentication(AuthenticationContext, String, Principal)
 * reevaluateAuthentication(..)} and
 * {@link SessionAuthenticator#reevaluateAuthenticationForOutgoingMessage(AuthenticationContext, String, Principal, long)
 * reevaluateAuthenticationForOutgoingMessage(..)} may be invoked several times.</li>
 * <li>When the Client tries to subscribe to a Topic, the method
 * {@link SessionAuthenticator#authorizeUserForTopic(AuthenticationContext, String) authorizeUserForTopic(..)} is
 * invoked to decide whether to allow or deny the subscription.</li>
 * </ol>
 * <b>Read through all methods' JavaDoc in succession to get an understanding of how it works!</b>
 * <p/>
 * <i>Thread Safety:</i> Concurrency issues wrt. to multiple threads accessing a {@link SessionAuthenticator}: Seen from
 * the {@link MatsSocketServer}, only one thread will ever access any of the methods at any one time, and memory
 * consistency is handled (i.e. the {@link SessionAuthenticator} instance is <i>effectively</i> synchronized on): You do
 * not need to synchronize on anything within an instance of {@link SessionAuthenticator}, and any fields set on the
 * instance by some method will be correctly available for subsequent invocations of the same or any other methods.
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
     *         singleton, or if a new instance is returned per invocation (which then means there is a unique instance
     *         per connection, thus you can hold values for the connection there).
     */
    SessionAuthenticator newSessionAuthenticator();

    /**
     * An instance of this interface shall be returned upon invocation of {@link #newSessionAuthenticator()}. The
     * implementation may choose whether it is a singleton, or if a new instance is returned per invocation (which then
     * means there is a unique instance per WebSocket/MatsSocket Session, thus you can hold values for the session
     * there).
     */
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
         * <b>The default implementation returns <code>true</code>, i.e. letting all connections through this step.</b>
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
         * <b>The default implementation returns <code>true</code>, i.e. letting all connections through this step.</b>
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
         * <b>The default implementation returns <code>true</code>, i.e. letting all connections through this step.</b>
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
         * Invoked when the MatsSocket initially connects (or reconnects) over WebSocket, and needs to be authenticated
         * by the Authorization string supplied via the initial set ("pipeline") of message from the client, typically
         * in the HELLO message (which needs to present in the initial pipeline). At this point, there is obviously do
         * not a Principal yet, so this {@link SessionAuthenticator} needs to supply it.
         * <p />
         * <b>For every next incoming pipeline of messages</b>, the method
         * {@link #reevaluateAuthentication(AuthenticationContext, String, Principal) reevaluateAuthentication(...)} is
         * invoked - which, depending on authentication scheme in use, might just always reply
         * {@link AuthenticationContext#stillValid() "stillValid"}, thus trusting the initial authentication - or for
         * e.g. OAuth schemes where the token has an expiry time, actually evaluate this expiry time. If that method
         * decides that the current Authorization value isn't good enough, the Client will be asked to provide a new one
         * (refresh the token).
         * <p />
         * <b>For outgoing messages from the Server</b> (that is, Replies to Client-to-Server Requests, and
         * Server-to-Client Sends and Requests), the method
         * {@link #reevaluateAuthenticationForOutgoingMessage(AuthenticationContext, String, Principal, long)} is
         * invoked. Again, it depends on the authentication scheme in use: Either trust the initial authentication, or
         * re-evaluate whether the current token which the Server has is valid enough. And again, if that method decides
         * that the current Authorization value isn't good enough, the Client will be asked to provide a new one
         * (refresh the token).
         * <p />
         * Note: If this method returns {@link AuthenticationContext#invalidAuthentication(String)}, the connection is
         * immediately closed with {@link MatsSocketCloseCodes#VIOLATED_POLICY}.
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
         * Invoked on every subsequent "pipeline" of incoming messages (including every time the Client supplies a new
         * Authorization string). The reason for separating this out in a different method is that you should want to
         * optimize it heavily (read 'Note' below): If the 'existingPrincipal' is still valid (and with that also the
         * userId that was supplied at {@link AuthenticationContext#authenticated(Principal, String)}), then you can
         * return the result of {@link AuthenticationContext#stillValid()}. A correct, albeit possibly slow,
         * implementation of this method is to just forward the call to
         * {@link #initialAuthentication(AuthenticationContext, String) initialAuthentication(..)} - it was however
         * decided to not default-implement this solution in the interface, so that you should consider the implications
         * of this.
         * <p />
         * If the method returns {@link AuthenticationContext#invalidAuthentication(String) invalidAuthentication()},
         * the server will not process the incoming pipeline, and instead ask the client for re-auth, and when this
         * comes in and is valid, the processing will ensue (it holds the already provided messages so that client does
         * not need to deliver them again). <b>Notice: As opposed to the
         * {@link #initialAuthentication(AuthenticationContext, String) initialAuthentication}, replying
         * invalidAuthentication() does NOT immediately close the WebSocket</b>, but only implies that the client needs
         * to supply a new authorization headers before processing will go on.
         * <p />
         * <b>NOTE!</b> You might want to hold on to the 'authorizationHeader' between the invocations to quickly
         * evaluate whether it has changed: In e.g. an OAuth setting, where the authorizationHeader is a bearer access
         * token, you could shortcut evaluation: If it has not changed, then you might be able to just evaluate whether
         * it has expired by comparing a timestamp that you stored when first evaluating it, towards the current time.
         * However, if the authorizationHeader (token) has changed, you would do full evaluation of it, as with
         * {@link #initialAuthentication(AuthenticationContext, String) initialAuthentication(..)}.
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

        /**
         * This method is invoked each time the server wants to send a message to the client - either a REPLY to a
         * request from the Client, or a Server-to-Client {@link MatsSocketServer#send(String, String, String, Object)
         * SEND} or {@link MatsSocketServer#request(String, String, String, Object, String, String, byte[])} REQUEST}.
         * <p />
         * If the method returns {@link AuthenticationContext#invalidAuthentication(String) invalidAuthentication()},
         * the server will now hold this delivery, and instead ask the client for re-auth, and when this comes in and is
         * valid, the delivery will ensue. <b>Notice: As opposed to the
         * {@link #initialAuthentication(AuthenticationContext, String) initialAuthentication}, replying
         * invalidAuthentication() does NOT immediately close the WebSocket</b>, but only implies that the client needs
         * to supply a new authorization headers before getting the outbound messages.
         * <p />
         * It is default-implemented to invoke
         * {@link #reevaluateAuthentication(AuthenticationContext, String, Principal) reevaluateAuthentication(..)}, but
         * the reason for separating this out in (yet a) different method is that you might want to add a bit more slack
         * wrt. expiry if OAuth-style auth is in play: Let's say a request from the Client is performed right at the
         * limit of the expiry of the token (where the "slack" is set to a ridiculously low 10 seconds). But let's now
         * imagine that this request takes 11 seconds to complete. If you do not override the default implementation,
         * the default implementation will forward to {@code reevaluateAuthentication}, get
         * {@link AuthenticationContext#invalidAuthentication(String) invalidAuthentication()} as answer, and thus
         * initiate a full round-trip to the client and to the auth-server to get a new token. However, considering that
         * this {@link SessionAuthenticator SessionAuthenticator} was OK with the authentication when the request came
         * it, and that by the situation's definition the WebSocket connection has not gone done in the mean time
         * (otherwise it would have had to do full initial auth), one could argue that a Reply should have some extra
         * room wrt. expiry. For Server-to-Client messages SEND and REQUEST, the same mechanism is employed, and I'd
         * argue that the same arguments hold: This {@code SessionAuthenticator} was happy with the authentication some
         * few minutes ago and the connection has not broken in the meantime, so you should work pretty hard to set up a
         * situation where a REQUEST from the Server or some data using SEND from the Server would be a large security
         * consideration.
         * <p/>
         * You get provided the last time this {@code SessionAuthenticator} was happy with this exact Authorization
         * value as parameter 'lastAuthenticatedTimestamp' - that is, this timestamp is reset each time
         * {@link #initialAuthentication(AuthenticationContext, String) initialAuthentication(..)} and
         * {@link #reevaluateAuthentication(AuthenticationContext, String, Principal) reevaluateAuthentication} answers
         * any of {@link AuthenticationContext#authenticated(Principal, String) authenticated} or
         * {@link AuthenticationContext#stillValid()}.
         * <p/>
         * An ok implementation is therefore to simply answer {@link AuthenticationContext#stillValid() stillValid()} if
         * the 'lastAuthenticatedTimestamp' parameter is less than X minutes ago, otherwise answer
         * {@link AuthenticationContext#invalidAuthentication(String) invalidAuthentication()} (and thus the client will
         * be asked to supply new auth). Another slightly more elaborate implementation is to store the token and the
         * actual expiry time of the token in {@link #reevaluateAuthentication(AuthenticationContext, String, Principal)
         * reevaluateAuthentication(..)}, and if the token is the same with this invocation, just evaluate the expiry
         * time against current time, but add X minutes slack to this evaluation.
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
         * @param lastAuthenticatedTimestamp
         *            the millis-since-epoch of when this exact 'authorizationHeader' was deemed OK by this same
         *            {@code SessionAuthenticator}.
         * @return an {@link AuthenticationResult}, which you get from any of the method on the
         *         {@link AuthenticationContext} - preferably either {@link AuthenticationContext#stillValid()
         *         stillValid()} or {@link AuthenticationContext#invalidAuthentication(String) invalidAuthentication()}
         *         based on whether to let this Server-to-Client message through, or force the Client to reauthenticate
         *         before getting the message.
         */
        default AuthenticationResult reevaluateAuthenticationForOutgoingMessage(AuthenticationContext context,
                String authorizationHeader, Principal existingPrincipal, long lastAuthenticatedTimestamp) {
            return reevaluateAuthentication(context, authorizationHeader, existingPrincipal);
        }

        /**
         * Decide whether the specified Principal/User should be allowed to subscribe to the specified Topic.
         * <p />
         * Note: The 'authorizationHeader' is already validated by one of the <i>authentication</i> methods, which have
         * supplied the Principal and userId present in the supplied {@link AuthenticationContext}.
         * <p />
         * <b>The default implementation return <code>true</code>, i.e. letting all users subscribe to all Topics.</b>
         *
         * @param context
         *            the {@link AuthenticationContext} for reference - this has getters for the info you should require
         *            to decide whether the current Client user should be allowed to subscribe to the given topic.
         * @param topicId
         *            the Id of the Topic the client tries to subscribe to
         * @return <code>true</code> if the user should be allowed to subscribe to the Topic, <code>false</code> if the
         *         user should not be allowed to subscribe - he will then not get any messages sent over the Topic.
         */
        default boolean authorizeUserForTopic(AuthenticationContext context, String topicId) {
            return true;
        }
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
         * @return the current MatsSocketSession. Until
         *         {@link SessionAuthenticator#initialAuthentication(AuthenticationContext, String) Initial
         *         Authentication} (happening with the HELLO message from the Client), all the
         *         <code>Optional</code>-returning methods will return <code>Optional.empty()</code> - it is basically
         *         the responsibility of the authentication mechanism to supply values for these.
         */
        LiveMatsSocketSession getMatsSocketSession();

        /**
         * Sets (or overrides) the Remote Address, as exposed via {@link ActiveMatsSocketSession#getRemoteAddr()} - read
         * that JavaDoc, in particular that if this server is behind a proxy, this will be the proxy's address. Also see
         * {@link #setOriginatingRemoteAddr(String)} where any X-Forwarded-For resolved address should be set.
         * <p />
         * <b>Note that if the MatsSocketServer handles getting the remote address itself (via hacks, since JSR 356 Java
         * API for WebSockets does not expose it), it will already be available in the
         * {@link #getMatsSocketSession()}.<b/>
         *
         * @param remoteAddr
         *            what should be replied by {@link ActiveMatsSocketSession#getRemoteAddr()}.
         * @see ActiveMatsSocketSession#getRemoteAddr()
         * @see #setOriginatingRemoteAddr(String)
         */
        void setRemoteAddr(String remoteAddr);

        /**
         * Sets the Originating Remote Address, as exposed via
         * {@link ActiveMatsSocketSession#getOriginatingRemoteAddr()} - read that JavaDoc.
         *
         * @param originatingRemoteAddr
         *            what should be replied by {@link ActiveMatsSocketSession#getOriginatingRemoteAddr()}.
         * @see ActiveMatsSocketSession#getOriginatingRemoteAddr()
         * @see #setRemoteAddr(String)
         */
        void setOriginatingRemoteAddr(String originatingRemoteAddr);

        // ===== Authentication Result return-value methods.

        /**
         * <b>Bad Authentication!</b> Return the result from this method from
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
        AuthenticationResult invalidAuthentication(String reason);

        /**
         * <b>Good Authentication!</b> Return the result from this method from
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
         *            {@link IncomingAuthorizationAndAdapter#handleIncoming(MatsSocketEndpointIncomingContext, Principal, Object)}
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
         * <b>Good Authentication!</b> Variant of {@link #authenticated(Principal, String)} that grants the
         * authenticated user special abilities to ask for debug info of the performed call.
         *
         * @param principal
         *            the Principal that will be supplied to all
         *            {@link IncomingAuthorizationAndAdapter#handleIncoming(MatsSocketEndpointIncomingContext, Principal, Object)}
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
                EnumSet<DebugOption> allowedDebugOptions);

        /**
         * <b>Existing Authentication is still good!</b> Return the result from this method from
         * {@link SessionAuthenticator#reevaluateAuthentication(AuthenticationContext, String, Principal)} if the
         * 'existingPrincipal' (and implicitly the userId) is still good to go.
         *
         * @return an {@link AuthenticationResult} that can be returned by the method
         *         {@link SessionAuthenticator#reevaluateAuthentication(AuthenticationContext, String, Principal)},
         *         stating that the existing authorization is still valid.
         */
        AuthenticationResult stillValid();
    }

    /**
     * These bit-field enums (bit-field) is used by the Client to request different types of debug/meta information from
     * Server-side, and used by the {@link AuthenticationPlugin} to tell the {@link MatsSocketServer} which types of
     * information the specific user is allowed to request - the resulting debug/meta information provided is the
     * intersection of the requested + allowed.
     * <p/>
     * In addition to the functionality from the MatsSocket system, there is two "Custom Options" ({@link #CUSTOM_A} and
     * {@link #CUSTOM_B}) that are not employed by the MatsSocket system, but instead can be utilized and given meaning
     * by the MatsSocket employing service/application. Notice, however: You might be just as well off by implementing
     * such functionality on the {@link Principal} returned by the {@link AuthenticationPlugin} ("this user is allowed
     * to request these things") - and on the request DTOs from the Client ("I would like to request these things").
     */
    enum DebugOption {
        /**
         * Timestamp info for the separate phases. Note that time-skew between different nodes must be taken into
         * account.
         */
        TIMESTAMPS(0b0000_0001),

        /**
         * Node-name of the handling nodes of the separate phases.
         */
        NODES(0b0000_0010),

        /**
         * {@link AuthenticationPlugin}-specific "Option A" - this is not used by MatsSocket itself, but can be employed
         * and given a meaning by the {@link AuthenticationPlugin}.
         * <p/>
         * Notice: You might be just as well off by implementing such functionality on the {@link Principal} returned by
         * the {@link AuthenticationPlugin} ("this user is allowed to request these things") - and on the request DTOs
         * from the Client ("I would like to request these things").
         */
        CUSTOM_A(0b0100_0000),

        /**
         * {@link AuthenticationPlugin}-specific "Option B" - this is not used by MatsSocket itself, but can be employed
         * and given a meaning by the {@link AuthenticationPlugin}.
         * <p/>
         * Notice: You might be just as well off by implementing such functionality on the {@link Principal} returned by
         * the {@link AuthenticationPlugin} ("this user is allowed to request these things") - and on the request DTOs
         * from the Client ("I would like to request these things").
         */
        CUSTOM_B(0b1000_0000);

        private final int bitconstant;

        DebugOption(int bitconstant) {
            this.bitconstant = bitconstant;
        }

        static public EnumSet<DebugOption> enumSetOf(int flags) {
            EnumSet<DebugOption> debugOptions = EnumSet.noneOf(DebugOption.class);
            for (DebugOption value : DebugOption.values()) {
                if ((flags & value.bitconstant) > 0) {
                    debugOptions.add(value);
                }
            }
            return debugOptions;
        }

        static public EnumSet<DebugOption> enumSetOf(Integer flags) {
            if (flags == null) {
                return EnumSet.noneOf(DebugOption.class);
            }
            // E-> had value
            return enumSetOf(flags.intValue());
        }

        static public int flags(EnumSet<DebugOption> debugOptions) {
            int bitfield = 0;
            for (DebugOption debugOption : debugOptions) {
                bitfield |= debugOption.bitconstant;
            }
            return bitfield;
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
