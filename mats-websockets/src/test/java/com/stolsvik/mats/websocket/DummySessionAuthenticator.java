package com.stolsvik.mats.websocket;

import java.security.Principal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationContext;
import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationResult;
import com.stolsvik.mats.websocket.AuthenticationPlugin.SessionAuthenticator;

/**
 * @author Endre Stølsvik 2020-02-23 14:14 - http://stolsvik.com/, endre@stolsvik.com
 */
class DummySessionAuthenticator implements SessionAuthenticator {
    private static final Logger log = LoggerFactory.getLogger(DummySessionAuthenticator.class);

    private String _currentGoodAuthorizationHeader;
    private long _currentExpiryMillis;

    @Override
    public AuthenticationResult initialAuthentication(AuthenticationContext context, String authorizationHeader) {
        // NOTICE! DO NOT LOG AUTHORIZATION HEADER IN PRODUCTION!!
        log.info("initialAuthentication(..): Resolving principal for Authorization Header [" + authorizationHeader
                + "].");
        String[] split = authorizationHeader.split(":");
        // ?: If it does not follow standard for DummySessionAuthenticator, fail
        if (split.length != 3) {
            // -> Not three parts -> Fail
            return context.invalidAuthentication("The Authorization should read 'DummyAuth:<userId>:<expiresMillis>':"
                    + " Supplied is not three parts.");
        }
        // ?: First part reads "DummyAuth"?
        if (!"DummyAuth".equals(split[0])) {
            // -> Not "DummyAuth" -> Fail
            return context.invalidAuthentication("The Authorization should read 'DummyAuth:<userId>:<expiresMillis>':"
                    + " First part is not 'DummyAuth'");
        }

        // ?: Is it a special auth string that wants to fail upon initialAuthentication?
        if (split[1].contains("fail_initialAuthentication")) {
            // -> Yes, special auth string
            return context.invalidAuthentication("Asked to fail in initialAuthentication(..)");

        }

        long expires = Long.parseLong(split[2]);
        if ((expires != -1) && (expires < System.currentTimeMillis())) {
            return context.invalidAuthentication("This DummyAuth is too old (initialAuthentication).");
        }

        String userId = split[1];

        // ----- We've evaluated the AuthorizationHeader, good to go!

        _currentGoodAuthorizationHeader = authorizationHeader;
        _currentExpiryMillis = expires;

        // Create Principal to return
        Principal princial = new DummyAuthPrincipal(userId, authorizationHeader);
        // This was a good authentication
        return context.authenticated(princial, userId);
    }

    @Override
    public AuthenticationResult reevaluateAuthentication(AuthenticationContext context, String authorizationHeader,
            Principal existingPrincipal) {
        // NOTICE! DO NOT LOG AUTHORIZATION HEADER IN PRODUCTION!!
        log.info("reevaluateAuthentication(..): Authorization Header [" + authorizationHeader + "].");
        // ?: Is it a special auth string that wants to fail upon reevaluateAuthentication?
        if (authorizationHeader.contains(":fail_reevaluateAuthentication:")) {
            // -> Yes, special auth string
            return context.invalidAuthentication("Asked to fail in reevaluateAuthentication(..)");

        }
        // ?: If the AuthorizationHeader has not changed, then just evaluate expiry
        if (_currentGoodAuthorizationHeader.equals(authorizationHeader)) {
            long expiryMillisLeft = _currentExpiryMillis - System.currentTimeMillis();
            // Evaluate current expiry time
            if ((_currentExpiryMillis != -1) && (expiryMillisLeft <= 0)) {
                log.warn("Current DummyAuth is too old (reevaluateAuthentication) - currentExpiry:["
                        + _currentExpiryMillis + "], which is [" + expiryMillisLeft + " ms] ago.");
                return context.invalidAuthentication("This DummyAuth is too old (reevaluateAuthentication).");
            }
            // Evidently was good, so still valid.
            log.info("Still valid auth, there is [" + expiryMillisLeft + " ms] left");
            return context.stillValid();
        }
        // E-> Changed auth header, so do full initialAuth
        return initialAuthentication(context, authorizationHeader);
    }

    @Override
    public AuthenticationResult reevaluateAuthenticationForOutgoingMessage(AuthenticationContext context,
            String authorizationHeader, Principal existingPrincipal, long lastAuthenticatedTimestamp) {
        // NOTICE! DO NOT LOG AUTHORIZATION HEADER IN PRODUCTION!!
        log.info("reevaluateAuthenticationForOutgoingMessage(..): Authorization Header [" + authorizationHeader
                + "], lastAuthenticatedMillis:[" + lastAuthenticatedTimestamp + "], which is [" + (System
                        .currentTimeMillis() - lastAuthenticatedTimestamp) + " ms] ago.");
        // ?: Is it a special auth string that wants to fail upon reevaluateAuthenticationForOutgoingMessage?
        if (authorizationHeader.contains(":fail_reevaluateAuthenticationForOutgoingMessage:")) {
            // -> Yes, special auth string
            return context.invalidAuthentication("Asked to fail in reevaluateAuthenticationForOutgoingMessage(..)");

        }
        // E-> No, so just forward to reevaluateAuthentication(..)
        return reevaluateAuthentication(context, authorizationHeader, existingPrincipal);
    }

    public static class DummyAuthPrincipal implements Principal {
        private final String _userId;
        private final String _authorizationHeader;

        public DummyAuthPrincipal(String userId, String authorizationHeader) {
            _userId = userId;
            _authorizationHeader = authorizationHeader;
        }

        public String getUserId() {
            return _userId;
        }

        public String getAuthorizationHeader() {
            return _authorizationHeader;
        }

        @Override
        public String getName() {
            return "userId:" + _userId;
        }

        @Override
        public String toString() {
            return "DummyPrincipal:[" + _authorizationHeader + ']';
        }
    }
}
