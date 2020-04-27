package com.stolsvik.mats.websocket.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import javax.websocket.RemoteEndpoint.Async;
import javax.websocket.Session;

/**
 * Provides several hacks to get the Remote Address for different Servlet Containers implementing JSR 356 Java API for
 * WebSockets, as that standard does not specify a method for it. Currently tries to get a Remote Address using the
 * following hacks (If you can provide more, let us know!):
 * <ul>
 * <li>Jetty (tested): Property on the User Properties map on WebSocket Session - check Jetty's class "JsrCreator":
 * <a href="https://stackoverflow.com/a/36028476/39334">https://stackoverflow.com/a/36028476/39334</a></li>
 * <li>Glassfish / Tyrus (not tested!): Reflect method "getRemoteAddr()" on WebSocket Session (which is a TyrusSession):
 * <a href="https://stackoverflow.com/a/25110850/39334">https://stackoverflow.com/a/25110850/39334</a></li>
 * <li>Tomcat (not tested!): Reflect a series of fields from Session.getAsyncRemote() (Session is a WsSession):
 * <a href="https://stackoverflow.com/a/32629586/39334">https://stackoverflow.com/a/32629586/39334</a></li>
 * </ul>
 *
 * @author Endre Stølsvik 2020-04-25 21:11 - http://stolsvik.com/, endre@stolsvik.com
 */
public class RemoteAddressContainerHacks {

    public static String attemptGetRemoteAddress(Session session) {
        // :: Jetty
        String jettyRemoteAddr = getJettyRemoteAddress(session);
        if (jettyRemoteAddr != null) {
            return jettyRemoteAddr;
        }

        // :: Tomcat
        // ... Older Tomcats
        InetSocketAddress tomcatRemoteAddr = getTomcatRemoteAddress(session,
                "base#sos#socketWrapper#socket#sc#remoteAddress");
        if (tomcatRemoteAddr != null) {
            return tomcatRemoteAddr.getAddress().getHostAddress();
        }
        // ... Newer Tomcats
        tomcatRemoteAddr = getTomcatRemoteAddress(session, "base#socketWrapper#socket#sc#remoteAddress");
        if (tomcatRemoteAddr != null) {
            return tomcatRemoteAddr.getAddress().getHostAddress();
        }

        // :: Glassfish / Tyrus:
        String tyrusRemoteAddr = getTyrusRemoteAddress(session);
        if (tyrusRemoteAddr != null) {
            return tyrusRemoteAddr;
        }

        return null;
    }

    // Attempt to get remote address from UserProperties, based on https://stackoverflow.com/a/36028476/39334
    private static String getJettyRemoteAddress(Session session) {
        try {
            InetSocketAddress jettyRemoteAddr = (InetSocketAddress) session.getUserProperties()
                    .get("javax.websocket.endpoint.remoteAddress");
            if (jettyRemoteAddr != null) {
                return jettyRemoteAddr.getAddress().getHostAddress();
            }
        }
        catch (Exception e) {
            /* Didn't go good, hopefully not Jetty (but what is it then, when property is non-null?!)..! */
        }
        return null;
    }

    // Attempt to get address from TyrusSession, based on explanation in https://stackoverflow.com/a/25110850/39334
    private static String getTyrusRemoteAddress(Session session) {
        try {
            Method getRemoteAddr = session.getClass().getMethod("getRemoteAddr");
            return (String) getRemoteAddr.invoke(session);
        }
        catch (Exception e) {
            /* Didn't go good, hopefully not Tyrus..! */
            return null;
        }
    }

    // Following methods gotten from https://stackoverflow.com/a/32629586/39334

    private static InetSocketAddress getTomcatRemoteAddress(Session session, String fieldToFind) {
        Async async = session.getAsyncRemote();
        return (InetSocketAddress) getFieldInstance(async, fieldToFind);
    }

    private static Object getFieldInstance(Object obj, String fieldPath) {
        String[] fields = fieldPath.split("#");
        for (String field : fields) {
            obj = getField(obj, obj.getClass(), field);
            if (obj == null) {
                return null;
            }
        }

        return obj;
    }

    private static Object getField(Object obj, Class<?> clazz, String fieldName) {
        for (; clazz != Object.class; clazz = clazz.getSuperclass()) {
            try {
                Field field;
                field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(obj);
            }
            catch (Exception e) {
                /* Didn't go good, hopefully not Tomcat..! */
            }
        }
        return null;
    }
}
