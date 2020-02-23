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
        // NOTICE! You do not want to log the Authorization Header in production!
        log.info("Initial Authentication, resolving principal for Authorization Header [" + authorizationHeader + "].");
        String[] split = authorizationHeader.split(":");
        // ?: If it does not follow standard for DummySessionAuthenticator, fail
        if (split.length != 3) {
            // -> Not three parts -> Fail
            return context.notAuthenticated("The Authorization should read 'DummyAuth:<userId>:<expiresMillis>':"
                    + " Supplied is not three parts.");
        }
        // ?: First part reads "DummyAuth"?
        if (!"DummyAuth".equals(split[0])) {
            // -> Not "DummyAuth" -> Fail
            return context.notAuthenticated("The Authorization should read 'DummyAuth:<userId>:<expiresMillis>':"
                    + " First part is not 'DummyAuth'");
        }

        long expires = Long.parseLong(split[2]);
        evaluateExpires(expires);

        String userId = split[1];

        // ----- We've evaluated the AuthorizationHeader, good to go!

        _currentGoodAuthorizationHeader = authorizationHeader;
        _currentExpiryMillis = expires;

        // Create Principal to return
        Principal princial = new DummyAuthPrincipal(userId, authorizationHeader);
        // This was a good authentication
        return context.authenticated(princial, userId);
    }

    private void evaluateExpires(long expires) {
        if ((expires != -1) && (expires < System.currentTimeMillis())) {
            throw new IllegalStateException("This DummyAuth is too old.");
        }
    }

    @Override
    public AuthenticationResult reevaluateAuthentication(AuthenticationContext context, String authorizationHeader,
            Principal existingPrincipal) {
        // NOTICE! You do not want to log the Authorization Header in production!
        log.info("Reevaluating Authentication for Authorization Header ["+authorizationHeader+"].");
        // ?: If the AuthorizationHeader has not changed, then just evaluate expiry
        if (_currentGoodAuthorizationHeader.equals(authorizationHeader)) {
            // This throws if not good
            evaluateExpires(_currentExpiryMillis);
            // Evidently was good, so still valid.
            return context.stillValid();
        }
        // E-> Changed auth header, so do full initialAuth
        return initialAuthentication(context, authorizationHeader);
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
