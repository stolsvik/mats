package com.stolsvik.mats.websocket;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpoint;

import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.webapp.WebXmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.test.Rule_Mats;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpoint;

/**
 * @author Endre Stølsvik 2019-11-21 21:07 - http://stolsvik.com/, endre@stolsvik.com
 */
public class AppMain {

    private static final Logger log = LoggerFactory.getLogger(AppMain.class);

    @WebListener
    public static class SCL_Endre implements ServletContextListener {

        private final Rule_Mats _matsRule = new Rule_Mats();

        @Override
        public void contextInitialized(ServletContextEvent sce) {
            log.info("EndreXY contextInitialized: Test 1 2 3: " + sce);
            _matsRule.before();

            MatsFactory matsFactory = _matsRule.getMatsFactory();

            // Make MatsEndpoint
            matsFactory.single("Test.single", MatsDataTO.class, MatsDataTO.class, (processContext, incomingDto) -> {
                return new MatsDataTO(incomingDto.number, incomingDto.string + ":FromSimple", incomingDto.multiplier);
            });

            // Create MatsSocketServer
            MatsSocketServer matsSocketServer = getMatsSocketServer(sce, matsFactory);

            // Make MatsSocketEndpoint
            MatsSocketEndpoint<MatsSocketRequestDto, MatsDataTO, MatsDataTO, MatsSocketReplyDto> matsSocketEndpoint = matsSocketServer
                    .matsSocketEndpoint("Test.single",
                            MatsSocketRequestDto.class, MatsDataTO.class, MatsSocketReplyDto.class);
            matsSocketEndpoint.incomingForwarder((ctx, msIncoming) -> {
                log.info("Got MatsSocket request on MatsSocket EndpointId: "
                        + ctx.getMatsSocketEndpointId());
                log.info(" \\- Authorization: " + ctx.getAuthorization());
                log.info(" \\- Principal:     " + ctx.getPrincipal());
                log.info(" \\- Message:       " + msIncoming);
                ctx.initiate(msg -> {
                    msg.to(ctx.getMatsSocketEndpointId())
                            .interactive()
                            .nonPersistent()
                            .setTraceProperty("requestTimestamp", msIncoming.requestTimestamp);
                    if (ctx.isRequest()) {
                        msg.request(new MatsDataTO(msIncoming.number, msIncoming.string));
                    }
                    else {
                        msg.send(new MatsDataTO(msIncoming.number, msIncoming.string));
                    }
                });
            });
            matsSocketEndpoint.replyAdapter((ctx, matsReply) -> {
                log.info("Adapting message: " + matsReply);
                return new MatsSocketReplyDto(matsReply.string.length(), matsReply.number,
                        ctx.getMatsContext().getTraceProperty("requestTimestamp", Long.class));
            });
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            log.info("EndreXY contextDestroyed: Test 1 2 3: " + sce);
            _matsRule.after();
        }
    }

    private static MatsSocketServer getMatsSocketServer(ServletContextEvent sce, MatsFactory matsFactory) {
        Object wsServerContainer = sce.getServletContext().getAttribute(ServerContainer.class.getName());
        if (!(wsServerContainer instanceof ServerContainer)) {
            throw new AssertionError("Did not find '" + ServerContainer.class.getName() + "' object"
                    + " in ServletContext, but [" + wsServerContainer + "].");
        }
        return DefaultMatsSocketServer.makeMatsSocketServer(
                (ServerContainer) wsServerContainer, matsFactory);
    }

    @WebServlet("/test")
    public static class TestServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.getWriter().println("Testing Servlet");
        }
    }

    public static String id(Object x) {
        return x.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(x));
    }

    @ServerEndpoint("/ws/json2")
    public static class TestWebSocket {
        @OnOpen
        public void myOnOpen(Session session, EndpointConfig endpointConfig) {
            log.info("WebSocket opened, session:" + session.getId() + ", endpointConfig:" + endpointConfig + ", this:"
                    + id(this));
        }

        @OnMessage
        public void myOnMessage(Session session, String txt) {
            log.info("WebSocket received message:" + txt + ", session:" + session.getId() + ", this:" + id(this));
        }

        @OnClose
        public void myOnClose(Session session, CloseReason reason) {
            log.info("WebSocket @OnClose, session:" + session.getId() + ", reason:" + reason.getReasonPhrase()
                    + ", this:" + id(this));
        }

        @OnError
        public void myOnError(Session session, Throwable t) {
            log.info("WebSocket @OnError, session:" + session.getId() + ", this:" + id(this), t);
        }
    }

    public static Server createServer(int port) {
        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/");
        webapp.setBaseResource(Resource.newClassPathResource("webapp"));
        webapp.setThrowUnavailableOnStartupException(true);

        // Override the default configurations, stripping down and adding AnnotationConfiguration.
        // https://www.eclipse.org/jetty/documentation/9.4.x/configuring-webapps.html
        // Note: The default resides in WebAppContext.DEFAULT_CONFIGURATION_CLASSES
        webapp.setConfigurations(new Configuration[] {
                // new WebInfConfiguration(),
                new WebXmlConfiguration(), // Evidently adds the DefaultServlet, as otherwise no read of "/webapp/"
                // new MetaInfConfiguration(),
                // new FragmentConfiguration(),
                new AnnotationConfiguration() // Adds Servlet annotation processing.
        });

        // :: Get Jetty to Scan project classes too: https://stackoverflow.com/a/26220672/39334
        // Find "this" location for current classes
        URL classes = AppMain.class.getProtectionDomain().getCodeSource().getLocation();
        // Set this location to be scanned.
        webapp.getMetaData().setWebInfClassesDirs(Collections.singletonList(Resource.newResource(classes)));

        Server server = new Server(port);
        server.setHandler(webapp);
        return server;
    }

    public static void main(String... args) throws Exception {
        String portS = System.getProperty("jetty.http.port", "8080");
        int port = Integer.parseInt(portS);
        Server server = createServer(port);

        log.info("EndreXY: Starting server.");

        server.start();

        server.dumpStdErr();

        server.join();
    }

}
