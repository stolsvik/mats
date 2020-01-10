package com.stolsvik.mats.websocket.impl;

import java.security.Principal;

import javax.websocket.Session;
import javax.websocket.server.HandshakeRequest;

import com.stolsvik.mats.websocket.impl.AuthenticationPlugin.AuthenticationContext;
import com.stolsvik.mats.websocket.impl.AuthenticationPlugin.AuthenticationResult;

/**
 * @author Endre Stølsvik 2020-01-10 10:17 - http://stolsvik.com/, endre@stolsvik.com
 */
class AuthenticationContextImpl implements AuthenticationContext {

    private final HandshakeRequest _handshakeRequest;
    private final Session _webSocketSession;

    public AuthenticationContextImpl(HandshakeRequest handshakeRequest, Session webSocketSession) {
        _handshakeRequest = handshakeRequest;
        _webSocketSession = webSocketSession;
    }

    @Override
    public HandshakeRequest getHandshakeRequest() {
        return _handshakeRequest;
    }

    @Override
    public Session getWebSocketSession() {
        return _webSocketSession;
    }

    @Override
    public AuthenticationResult notAuthenticated(String reason) {
        return new AuthenticationResult_NotAuthenticated();
    }

    @Override
    public AuthenticationResult authenticated(Principal principal, String userId) {
        return new AuthenticationResult_Authenticated(principal, userId);
    }

    @Override
    public AuthenticationResult stillValid() {
        return new AuthenticationResult_StillValid();
    }

    static class AuthenticationResult_Authenticated implements AuthenticationResult {
        final Principal _principal;
        final String _userId;

        public AuthenticationResult_Authenticated(Principal principal, String userId) {
            _principal = principal;
            _userId = userId;
        }
    }

    static class AuthenticationResult_StillValid implements AuthenticationResult {

    }
    static class AuthenticationResult_NotAuthenticated implements AuthenticationResult {

    }
}