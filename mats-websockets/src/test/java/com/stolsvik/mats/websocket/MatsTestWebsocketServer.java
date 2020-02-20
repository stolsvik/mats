package com.stolsvik.mats.websocket;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.Collections;
import java.util.concurrent.ForkJoinPool;

import javax.jms.ConnectionFactory;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebListener;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.websocket.server.ServerContainer;

import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.component.AbstractLifeCycle.AbstractLifeCycleListener;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.webapp.WebXmlConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;
import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationContext;
import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationResult;
import com.stolsvik.mats.websocket.AuthenticationPlugin.SessionAuthenticator;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpoint;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward_SQL;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward_SQL.Database;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer;

import ch.qos.logback.core.CoreConstants;

/**
 * @author Endre Stølsvik 2019-11-21 21:07 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsTestWebsocketServer {

    private static final String CONTEXT_ATTRIBUTE_PORTNUMBER = "ServerPortNumber";
    private static final String CONTEXT_ATTRIBUTE_MATSSOCKETJS_PATH = "Path to MatsSocket.js";
    private static final String CONTEXT_ATTRIBUTE_MATSSOCKETTEST_PATH = "Path to test folder";

    private static final String WEBSOCKET_PATH = "/matssocket";

    private static final String COMMON_AMQ_NAME = "CommonAMQ";

    private static final Logger log = LoggerFactory.getLogger(MatsTestWebsocketServer.class);

    @WebListener
    public static class SCL_Endre implements ServletContextListener {

        private MatsSocketServer _matsSocketServer;
        private MatsFactory _matsFactory;

        @Override
        public void contextInitialized(ServletContextEvent sce) {
            log.info("ServletContextListener.contextInitialized(...): " + sce);
            log.info("  \\- ServletContext: " + sce.getServletContext());

            // ## Create MatsFactory
            // ActiveMQ ConnectionFactory
            ConnectionFactory connectionFactory = MatsLocalVmActiveMq.createConnectionFactory(COMMON_AMQ_NAME);
            // MatsSerializer
            MatsSerializer_DefaultJson matsSerializer = new MatsSerializer_DefaultJson();
            // Create the MatsFactory
            _matsFactory = JmsMatsFactory.createMatsFactory_JmsOnlyTransactions(
                    this.getClass().getSimpleName(), "*testing*",
                    new JmsMatsJmsSessionHandler_Pooling((s) -> connectionFactory.createConnection()),
                    matsSerializer);
            // Configure the MatsFactory for testing (remember, we're running two instances in same JVM)
            // .. Concurrency of only 1
            _matsFactory.getFactoryConfig().setConcurrency(1);
            // .. Use port number of current server as postfix for name of MatsFactory, and of nodename
            Integer portNumber = (Integer) sce.getServletContext().getAttribute(CONTEXT_ATTRIBUTE_PORTNUMBER);
            _matsFactory.getFactoryConfig().setName("MF_Server_" + portNumber);
            _matsFactory.getFactoryConfig().setNodename("EndreBox_" + portNumber);

            // :: Make simple single Mats Endpoint
            _matsFactory.single("Test.single", MatsDataTO.class, MatsDataTO.class, (processContext, incomingDto) -> {
                return new MatsDataTO(incomingDto.number, incomingDto.string + ":FromSimple", incomingDto.multiplier);
            });

            // ## Create MatsSocketServer
            // Create DataSource using H2
            JdbcDataSource h2Ds = new JdbcDataSource();
            h2Ds.setURL("jdbc:h2:~/temp/matsproject_dev_h2database/matssocket_dev;AUTO_SERVER=TRUE");
            JdbcConnectionPool dataSource = JdbcConnectionPool.create(h2Ds);

            // Create SQL-based ClusterStoreAndForward
            ClusterStoreAndForward_SQL clusterStoreAndForward = ClusterStoreAndForward_SQL.create(dataSource,
                    _matsFactory.getFactoryConfig().getNodename(), Database.MS_SQL);

            // Make a Dummy Authentication plugin
            AuthenticationPlugin authenticationPlugin = DummySessionAuthenticator::new;

            // Fetch the WebSocket ServerContainer
            ServerContainer wsServerContainer = (ServerContainer) sce.getServletContext()
                    .getAttribute(ServerContainer.class.getName());

            // Create the MatsSocketServer, piecing together the four needed elements + websocket mount point
            _matsSocketServer = DefaultMatsSocketServer.createMatsSocketServer(
                    wsServerContainer, _matsFactory, clusterStoreAndForward, authenticationPlugin, WEBSOCKET_PATH);

            // Set back the MatsSocketServer into ServletContext, to be able to shut it down properly.
            sce.getServletContext().setAttribute(MatsSocketServer.class.getName(), _matsSocketServer);

            // Set up all the MatsSocket Test Endpoints (used for integration tests, and the HTML test pages)
            TestMatsSocketEndpoints.setupMatsSocketEndpoints(_matsSocketServer);
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            log.info("ServletContextListener.contextDestroyed(..): " + sce);
            log.info("  \\- ServletContext: " + sce.getServletContext());
            _matsSocketServer.stop(5000);
            _matsFactory.stop(5000);
        }
    }

    private static class DummySessionAuthenticator implements SessionAuthenticator {
        @Override
        public AuthenticationResult initialAuthentication(AuthenticationContext context, String authorizationHeader) {
            log.info("Resolving Authorization header to principal for header [" + authorizationHeader + "].");
            long expires = Long.parseLong(authorizationHeader.substring(authorizationHeader.indexOf(':') + 1));
            if (expires < System.currentTimeMillis()) {
                throw new IllegalStateException("This DummyAuth is too old.");
            }
            Principal princial = new Principal() {
                @Override
                public String getName() {
                    return "Mr. Dummy Auth";
                }

                @Override
                public String toString() {
                    return "DummyPrincipal:" + authorizationHeader;
                }
            };
            return context.authenticated(princial, "endre");
        }

        @Override
        public AuthenticationResult reevaluateAuthentication(AuthenticationContext context, String authorizationHeader,
                Principal existingPrincipal) {
            return context.stillValid();
        }
    }

    /**
     * Servlet to shut down this entire Test Server. Employed from the Gradle integration tests.
     */
    @WebServlet("/shutdown")
    public static class ShutdownServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.getWriter().println("Shutting down");

            // Shut down the process
            ForkJoinPool.commonPool().submit(() -> System.exit(0));
        }
    }

    /**
     * Servlet mounted on the same path as the WebSocket - this actually works.
     */
    @WebServlet(WEBSOCKET_PATH)
    public static class TestServletSamePath extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.getWriter().println("Testing Servlet on same path as WebSocket");
        }
    }

    /**
     * Servlet that handles out-of-band close_session, which is invoked upon window.onunload by sendBeacon. The idea
     * is to get the MatsSocket Session closed even if the WebSocket channel is closed at the time.
     */
    @WebServlet("/matssocket/close_session")
    public static class OutOfBandCloseSessionServlet extends HttpServlet {
        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String sessonId = req.getParameter("session_id");
            MatsSocketServer matsSocketServer = (MatsSocketServer) req.getServletContext().getAttribute(
                    MatsSocketServer.class.getName());
            matsSocketServer.closeSession(sessonId);
        }
    }

    /**
     * Servlet to supply the MatsSocket.js file - this only works in development (i.e. running from e.g. IntelliJ).
     */
    @WebServlet("/mats/MatsSocket.js")
    public static class MatsSocketLibServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String dirForMatsSocket = (String) req.getServletContext().getAttribute(
                    CONTEXT_ATTRIBUTE_MATSSOCKETJS_PATH);
            if (dirForMatsSocket == null) {
                resp.sendError(501,
                        "Cannot find the MatsSocket.js file (path not existing) - this only works when in development.");
                return;
            }

            Path pathWithFile = Paths.get(dirForMatsSocket, "MatsSocket.js");
            if (!Files.exists(pathWithFile)) {
                resp.sendError(501,
                        "Cannot find the MatsSocket.js file (file not found) - this only works when in development.");
                return;
            }
            log.info("MatsSocket.js path: " + pathWithFile);

            resp.setContentType("application/javascript");
            resp.setCharacterEncoding("UTF-8");
            resp.setContentLengthLong(Files.size(pathWithFile));
            // Copy over the File to the HTTP Response's OutputStream
            InputStream inputStream = Files.newInputStream(pathWithFile);
            ServletOutputStream outputStream = resp.getOutputStream();
            int n;
            byte[] buffer = new byte[16384];
            while ((n = inputStream.read(buffer)) > -1) {
                outputStream.write(buffer, 0, n);
            }
        }
    }

    /**
     * Servlet to supply the test files - this only works in development (i.e. running from e.g. IntelliJ).
     */
    @WebServlet("/tests/*")
    public static class MatsUnitTestsServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String dirForTests = (String) req.getServletContext().getAttribute(
                    CONTEXT_ATTRIBUTE_MATSSOCKETTEST_PATH);
            if (dirForTests == null) {
                resp.sendError(501, "Cannot find the tests directory (path not existing) -"
                        + " this only works when in development.");
                return;
            }

            Path pathWithFile = Paths.get(dirForTests, req.getPathInfo());
            if (!Files.exists(pathWithFile)) {
                resp.sendError(404, "Cannot find the test file [" + req.getPathInfo() + "].");
                return;
            }
            log.info(req.getPathInfo() + " path: " + pathWithFile);

            resp.setContentType("application/javascript");
            resp.setCharacterEncoding("UTF-8");
            resp.setContentLengthLong(Files.size(pathWithFile));
            // Copy over the File to the HTTP Response's OutputStream
            InputStream inputStream = Files.newInputStream(pathWithFile);
            ServletOutputStream outputStream = resp.getOutputStream();
            int n;
            byte[] buffer = new byte[16384];
            while ((n = inputStream.read(buffer)) > -1) {
                outputStream.write(buffer, 0, n);
            }
        }
    }

    public static String id(Object x) {
        return x.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(x));
    }

    public static Server createServer(int port) {
        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setBaseResource(Resource.newClassPathResource("webapp"));
        // If any problems starting context, then let exception through so that we can exit.
        webAppContext.setThrowUnavailableOnStartupException(true);
        // Store the port number this server shall run under in the ServletContext.
        webAppContext.getServletContext().setAttribute(CONTEXT_ATTRIBUTE_PORTNUMBER, port);

        // Override the default configurations, stripping down and adding AnnotationConfiguration.
        // https://www.eclipse.org/jetty/documentation/9.4.x/configuring-webapps.html
        // Note: The default resides in WebAppContext.DEFAULT_CONFIGURATION_CLASSES
        webAppContext.setConfigurations(new Configuration[] {
                // new WebInfConfiguration(),
                new WebXmlConfiguration(), // Evidently adds the DefaultServlet, as otherwise no read of "/webapp/"
                // new MetaInfConfiguration(),
                // new FragmentConfiguration(),
                new AnnotationConfiguration() // Adds Servlet annotation processing.
        });

        // :: Get Jetty to Scan project classes too: https://stackoverflow.com/a/26220672/39334
        // Find location for current classes
        URL classesLocation = MatsTestWebsocketServer.class.getProtectionDomain().getCodeSource().getLocation();
        // Set this location to be scanned.
        webAppContext.getMetaData().setWebInfClassesDirs(Collections.singletonList(Resource.newResource(
                classesLocation)));

        // :: Find the path to the MatsSocket.js file
        String pathToClasses = classesLocation.getPath();
        // .. strip down to the 'mats-websockets' path
        int pos = pathToClasses.indexOf("mats-websockets");
        String pathToMatsSocket = pos == -1
                ? null
                : pathToClasses.substring(0, pos) + "mats-websockets/client/javascript";
        webAppContext.getServletContext().setAttribute(CONTEXT_ATTRIBUTE_MATSSOCKETJS_PATH, pathToMatsSocket + "/lib");
        webAppContext.getServletContext().setAttribute(CONTEXT_ATTRIBUTE_MATSSOCKETTEST_PATH, pathToMatsSocket
                + "/test");

        // Create the actual Jetty Server
        Server server = new Server(port);

        // Add StatisticsHandler (to enable graceful shutdown), put in the WebApp Context
        StatisticsHandler stats = new StatisticsHandler();
        stats.setHandler(webAppContext);
        server.setHandler(stats);

        // Add a Jetty Lifecycle Listener to cleanly shut down the MatsSocketServer.
        server.addLifeCycleListener(new AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStopping(LifeCycle event) {
                log.info("server.lifeCycleStopping for " + port + ", event:" + event + ", WebAppContext:"
                        + webAppContext + ", servletContext:" + webAppContext.getServletContext());
                MatsSocketServer matsSocketServer = (MatsSocketServer) webAppContext.getServletContext().getAttribute(
                        MatsSocketServer.class.getName());
                log.info("MatsSocketServer instance:" + matsSocketServer);
                matsSocketServer.stop(5000);
            }
        });

        // :: Graceful shutdown
        server.setStopTimeout(1000);
        server.setStopAtShutdown(true);
        return server;
    }

    public static void main(String... args) throws Exception {
        // Turn off LogBack's absurd SCI
        System.setProperty(CoreConstants.DISABLE_SERVLET_CONTAINER_INITIALIZER_KEY, "true");

        // Create common AMQ
        MatsLocalVmActiveMq inVmActiveMq = MatsLocalVmActiveMq.createInVmActiveMq(COMMON_AMQ_NAME);

        // Read in the server count as an argument, or assume 2
        int serverCount = (args.length > 0) ? Integer.parseInt(args[0]) : 2;
        // Read in start port to count up from, defaulting to 8080
        int nextPort = (args.length > 1) ? Integer.parseInt(args[0]) : 8080;

        // Start the desired number of servers
        Server[] servers = new Server[serverCount];
        for (int i = 0; i < servers.length; i++) {
            int serverId = i + 1;

            // Keep looping until we have found a free port that the server was able to start on
            while (true) {
                int port = nextPort;
                servers[i] = createServer(port);
                log.info("######### Starting server [" + serverId + "] on [" + port + "]");

                // Add a life cycle hook to log when the server has started
                servers[i].addLifeCycleListener(new AbstractLifeCycleListener() {
                    @Override
                    public void lifeCycleStarted(LifeCycle event) {
                        log.info("######### Started server " + serverId + " on port " + port);
                        // Using System.out to ensure that we get this out, even if logger is ERROR or OFF
                        System.out.println("HOOK_FOR_GRADLE_WEBSOCKET_URL: #[ws://localhost:" + port + WEBSOCKET_PATH
                                + "]#");
                    }
                });

                // Try and start the server on the port we set. If this fails, we will increment the port number
                // and try again.
                try {
                    servers[i].start();
                    break;
                }
                catch (IOException e) {
                    // ?: Check IOException's message whether we failed to bind to the port
                    if (e.getMessage().contains("Failed to bind")) {
                        // Yes -> Log, and try the next port by looping again
                        log.info("######### Failed to start server [" + serverId
                                + "] on [" + port + "], trying next port.", e);
                    }
                    else {
                        // No -> Some other IOException, re-throw to stop the server from starting.
                        throw e;
                    }
                }
                catch (Exception e) {
                    log.error("Jetty failed to start. Need to forcefully System.exit(..) due to Jetty not"
                            + " cleanly taking down its threads.", e);
                    System.exit(2);
                }
                finally {
                    // Always increment the port number
                    nextPort++;
                }
            }
        }
    }
}
