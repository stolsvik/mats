package com.stolsvik.mats.websocket.impl;

import java.security.Principal;
import java.util.EnumSet;

import javax.websocket.Session;
import javax.websocket.server.HandshakeRequest;

import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationContext;
import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationResult;
import com.stolsvik.mats.websocket.AuthenticationPlugin.DebugOption;
import com.stolsvik.mats.websocket.MatsSocketServer.LiveMatsSocketSession;

/**
 * @author Endre Stølsvik 2020-01-10 10:17 - http://stolsvik.com/, endre@stolsvik.com
 */
class AuthenticationContextImpl implements AuthenticationContext {

    private final HandshakeRequest _handshakeRequest;
    private final LiveMatsSocketSession _liveMatsSocketSession;

    public AuthenticationContextImpl(HandshakeRequest handshakeRequest, LiveMatsSocketSession liveMatsSocketSession) {
        _handshakeRequest = handshakeRequest;
        _liveMatsSocketSession = liveMatsSocketSession;
    }

    @Override
    public HandshakeRequest getHandshakeRequest() {
        return _handshakeRequest;
    }

    @Override
    public LiveMatsSocketSession getMatsSocketSession() {
        return _liveMatsSocketSession;
    }

    @Override
    public AuthenticationResult invalidAuthentication(String reason) {
        return new AuthenticationResult_InvalidAuthentication(reason);
    }

    @Override
    public AuthenticationResult authenticated(Principal principal, String userId) {
        return new AuthenticationResult_Authenticated(principal, userId);
    }

    @Override
    public AuthenticationResult authenticated(Principal principal, String userId,
            EnumSet<DebugOption> allowedDebugOptions) {
        return new AuthenticationResult_Authenticated(principal, userId, allowedDebugOptions);
    }

    @Override
    public AuthenticationResult stillValid() {
        return new AuthenticationResult_StillValid();
    }

    static class AuthenticationResult_Authenticated implements AuthenticationResult {
        final Principal _principal;
        final String _userId;
        final EnumSet<DebugOption> _debugOptions;

        public AuthenticationResult_Authenticated(Principal principal, String userId) {
            _principal = principal;
            _userId = userId;
            _debugOptions = EnumSet.noneOf(DebugOption.class);
        }

        public AuthenticationResult_Authenticated(Principal principal, String userId,
                EnumSet<DebugOption> debugOptions) {
            _principal = principal;
            _userId = userId;
            _debugOptions = debugOptions;
        }
    }

    static class AuthenticationResult_StillValid implements AuthenticationResult {

    }

    static class AuthenticationResult_InvalidAuthentication implements AuthenticationResult {
        private final String _reason;

        public AuthenticationResult_InvalidAuthentication(String reason) {
            _reason = reason;
        }

        public String getReason() {
            return _reason;
        }

        @Override
        public String toString() {
            return "AuthenticationResult_NotAuthenticated[" + _reason + ']';
        }
    }
}