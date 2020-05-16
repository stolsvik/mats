package com.stolsvik.mats.websocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.ConnectionFactory;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebListener;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.websocket.Session;
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

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;
import com.stolsvik.mats.websocket.MatsSocketServer.ActiveMatsSocketSession;
import com.stolsvik.mats.websocket.MatsSocketServer.ActiveMatsSocketSessionDto;
import com.stolsvik.mats.websocket.MatsSocketServer.LiveMatsSocketSession;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketSessionDto;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward_SQL;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward_SQL_DbMigrations;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward_SQL_DbMigrations.Database;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer;

import ch.qos.logback.core.CoreConstants;

/**
 * @author Endre Stølsvik 2019-11-21 21:07 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsTestWebsocketServer {

    private static final String CONTEXT_ATTRIBUTE_PORTNUMBER = "ServerPortNumber";
    private static final String CONTEXT_ATTRIBUTE_JAVASCRIPT_PATH = "Path to JavaScript files";

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
                    MatsTestWebsocketServer.class.getSimpleName(), "*testing*",
                    JmsMatsJmsSessionHandler_Pooling.create(connectionFactory),
                    matsSerializer);
            // Configure the MatsFactory for testing (remember, we're running two instances in same JVM)
            // .. Concurrency of only 1
            _matsFactory.getFactoryConfig().setConcurrency(1);
            // .. Use port number of current server as postfix for name of MatsFactory, and of nodename
            Integer portNumber = (Integer) sce.getServletContext().getAttribute(CONTEXT_ATTRIBUTE_PORTNUMBER);
            _matsFactory.getFactoryConfig().setName("MF_Server_" + portNumber);
            _matsFactory.getFactoryConfig().setNodename("EndreBox_" + portNumber);

            // ## Create MatsSocketServer
            // Create DataSource using H2
            JdbcDataSource h2Ds = new JdbcDataSource();
            h2Ds.setURL("jdbc:h2:~/temp/matsproject_dev_h2database/matssocket_dev;AUTO_SERVER=TRUE");
            JdbcConnectionPool dataSource = JdbcConnectionPool.create(h2Ds);

            // Create SQL-based ClusterStoreAndForward
            ClusterStoreAndForward_SQL clusterStoreAndForward = ClusterStoreAndForward_SQL.create(dataSource,
                    _matsFactory.getFactoryConfig().getNodename());
            // .. Perform DB migrations for the CSAF.
            ClusterStoreAndForward_SQL_DbMigrations.create(Database.MS_SQL).migrateUsingFlyway(dataSource);

            // Make a Dummy Authentication plugin
            AuthenticationPlugin authenticationPlugin = DummySessionAuthenticator::new;

            // Fetch the WebSocket ServerContainer from the ServletContainer (JSR 356 specific tie-in to Servlets)
            ServerContainer wsServerContainer = (ServerContainer) sce.getServletContext()
                    .getAttribute(ServerContainer.class.getName());

            // Create the MatsSocketServer, piecing together the four needed elements + websocket mount point
            _matsSocketServer = DefaultMatsSocketServer.createMatsSocketServer(
                    wsServerContainer, _matsFactory, clusterStoreAndForward, authenticationPlugin, WEBSOCKET_PATH);

            // Set back the MatsSocketServer into ServletContext, to be able to shut it down properly.
            // (Hack for Jetty's specific shutdown procedure)
            sce.getServletContext().setAttribute(MatsSocketServer.class.getName(), _matsSocketServer);

            // Set up all the Mats and MatsSocket Test Endpoints (used for integration tests, and the HTML test pages)
            SetupTestMatsAndMatsSocketEndpoints.setupMatsAndMatsSocketEndpoints(_matsFactory, _matsSocketServer);
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            log.info("ServletContextListener.contextDestroyed(..): " + sce);
            log.info("  \\- ServletContext: " + sce.getServletContext());
            _matsSocketServer.stop(5000);
            _matsFactory.stop(5000);
        }
    }

    /**
     * Servlet utilizing and displaying the introspection functionality of MatsSocketServer.
     */
    @WebServlet("/introspection")
    public static class IntrospectionServlet extends HttpServlet {

        private static DateTimeFormatter SDF = DateTimeFormatter.ISO_DATE_TIME;

        private String dateTime(Instant instant) {
            return SDF.format(ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()));
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            long nanos_total_start = System.nanoTime();
            resp.setContentType("text/html");
            resp.setCharacterEncoding("UTF-8");

            MatsSocketServer matsSocketServer = (MatsSocketServer) req.getServletContext()
                    .getAttribute(MatsSocketServer.class.getName());

            long nanos_get_start = System.nanoTime();
            Map<String, LiveMatsSocketSession> activeMatsSocketSessions = matsSocketServer
                    .getLiveMatsSocketSessions();
            long nanos_get_taken = System.nanoTime() - nanos_get_start;

            PrintWriter writer = resp.getWriter();
            writer.println("<html><body><h1>Introspection</h1>");

            writer.println("<table>");
            activeMatsSocketSessions.forEach((sessionId, session) -> {
                writer.println("<tr>");

                writer.println("<td>" + sessionId + "</td>");
                writer.println("<td>" + session.getUserId() + "</td>");
                writer.println("<td>" + session.getPrincipalName().orElse("[Principal is gone!]")
                        + "</td>");
                writer.println("<td>" + dateTime(session.getLastAuthenticatedTimestamp()) + "</td>");
                writer.println("<td>" + dateTime(session.getLastClientPingTimestamp()) + "</td>");
                writer.println("<td>" + dateTime(session.getLastActivityTimestamp()) + "</td>");
                writer.println("<td>" + session.getAuthorization().orElse("[Authorization is gone!]") + "</td>");
                writer.println("<td>" + session.getAppName() + " : " + session.getAppVersion() + "</td>");
                writer.println("<td>" + session.getClientLibAndVersions() + "</td>");

                writer.println("</tr>");
            });
            writer.println("</table>");

            long nanos_total_taken = System.nanoTime() - nanos_total_start;

            writer.println("Total nanos: [" + nanos_total_taken + "], getNodeLocalActiveMatsSocketSessions() nanos:["
                    + nanos_get_taken + "].");

            writer.println("</body></html>");
        }
    }

    @WebServlet("/MatsSocketSessions.json")
    public static class MatsSocketSessions_Json_Servlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");

            MatsSocketServer matsSocketServer = (MatsSocketServer) req.getServletContext()
                    .getAttribute(MatsSocketServer.class.getName());

            // Create the Jackson ObjectMapper - using fields, not methods (like Mats and MatsSocket does).
            ObjectMapper mapper = new ObjectMapper();
            // Read and write any access modifier fields (e.g. private)
            mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
            mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
            // Drop nulls and Optional.empty()
            mapper.setSerializationInclusion(Include.NON_ABSENT);

            List<MatsSocketSessionDto> sessions = matsSocketServer
                    .getMatsSocketSessions(false, null, null, null);

            resp.setHeader("X-Session-Count", "" + matsSocketServer.getMatsSocketSessionsCount(false, null, null,
                    null));

            mapper.writeValue(resp.getWriter(), sessions);
        }
    }

    @WebServlet("/ActiveMatsSocketSessions.json")
    public static class ActiveMatsSocketSessions_Json_Servlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");

            MatsSocketServer matsSocketServer = (MatsSocketServer) req.getServletContext()
                    .getAttribute(MatsSocketServer.class.getName());

            // Create the Jackson ObjectMapper - using fields, not methods (like Mats and MatsSocket does).
            ObjectMapper mapper = new ObjectMapper();
            // Read and write any access modifier fields (e.g. private)
            mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
            mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
            // Drop nulls and Optional.empty()
            mapper.setSerializationInclusion(Include.NON_ABSENT);

            List<ActiveMatsSocketSessionDto> sessions = new ArrayList<>(matsSocketServer
                    .getActiveMatsSocketSessions().values());

            mapper.writeValue(resp.getWriter(), sessions);
        }
    }

    @WebServlet("/LiveMatsSocketSessions.json")
    public static class LiveMatsSocketSessions_Json_Servlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            long nanos_total_start = System.nanoTime();
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");

            MatsSocketServer matsSocketServer = (MatsSocketServer) req.getServletContext()
                    .getAttribute(MatsSocketServer.class.getName());

            // Create the Jackson ObjectMapper - using methods of the interface, not the instance's fields.
            ObjectMapper mapper = new ObjectMapper();
            // Write e.g. Dates as "1975-03-11" instead of timestamp, and instead of array-of-ints [1975, 3, 11].
            // Uses ISO8601 with milliseconds and timezone (if present).
            mapper.registerModule(new JavaTimeModule());
            mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            // Handle Optional, OptionalLong, OptionalDouble
            mapper.registerModule(new Jdk8Module());
            // Drop nulls and Optional.empty()
            mapper.setSerializationInclusion(Include.NON_ABSENT);
            // :: handle some fields which we want/need to ignore or tailor
            // To filter, this one shows three approaches: https://stackoverflow.com/a/46391039/39334
            // To just remove them, could have used https://stackoverflow.com/a/49010463/39334

            // Make a Jackson mixin for ignoring getWebSocketSession() and getActiveMatsSocketSession()
            abstract class AnnotatedActiveMatsSocketSession implements LiveMatsSocketSession {
                @JsonIgnore
                abstract public Session getWebSocketSession();

                @JsonIgnore
                abstract public ActiveMatsSocketSession getActiveMatsSocketSession();
            }
            mapper.addMixIn(ActiveMatsSocketSession.class, AnnotatedActiveMatsSocketSession.class);

            // Make a Jackson mixin for tailoring Principal's output to only output toString().
            // Note: Could have used @JsonSerialize with custom serializer for getPrincipal() on the above session mixin
            abstract class AnnotatedPrincipal implements Principal {
                @JsonValue
                abstract public String toString();
            }
            mapper.addMixIn(Principal.class, AnnotatedPrincipal.class);

            // Get ObjectWriter for a List of the /interface/ ActiveMatsSocketSession, not the instance class.
            // Ref: https://stackoverflow.com/a/54594839/39334
            ObjectWriter objectWriter = mapper.writerFor(TypeFactory.defaultInstance().constructType(
                    new TypeReference<List<LiveMatsSocketSession>>() {
                    })).withDefaultPrettyPrinter();

            long nanos_get_start = System.nanoTime();
            Map<String, LiveMatsSocketSession> activeMatsSocketSessions = matsSocketServer
                    .getLiveMatsSocketSessions();
            long nanos_get_taken = System.nanoTime() - nanos_get_start;
            List<LiveMatsSocketSession> sessions = new ArrayList<>(activeMatsSocketSessions.values());

            PrintWriter writer = resp.getWriter();
            objectWriter.writeValue(writer, sessions);

            long nanos_total_taken = System.nanoTime() - nanos_total_start;

            log.info("Output JSON of LiveMatsSocketSessions: Total nanos: [" + nanos_total_taken
                    + "], getLiveMatsSocketSessions() nanos:[" + nanos_get_taken + "].");
        }
    }

    /**
     * PreConnectOperation: Servlet mounted on the same path as the WebSocket, picking up any "Authorization:" header
     * and putting it in a Cookie named {@link DummySessionAuthenticator#AUTHORIZATION_COOKIE_NAME}. The point here is
     * that it is not possible to add an "Authorization" header to a WebSocket connection from a web browser, so if you
     * do want to have some kind of "early-authentication" for the initial HTTP REQUEST, you need to roll with what is
     * possible, which is that the initial WebSocket "Handshake" connection will still supply the host-and-path specific
     * Cookies the browser have in its cookie-jar upon connect - so if we get the auth-header into a cookie, we can
     * check that instead. Setting this cookie via client side javascript is not possible if the WebSocket URL is on a
     * different host. So we make a Servlet on the same host as the WebSocket which "moves over" the Authentication
     * header to a cookie.
     */
    @WebServlet(WEBSOCKET_PATH)
    public static class PreConnectAuthorizationHeaderToCookieServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
            log.info("PreConnectOperation - GET: Authorization header: "
                    + (null != req.getHeader("Authorization") ? "present" : "NOT present!")
                    + ", Origin: " + req.getHeader("Origin")
                    + ", path: " + req.getContextPath() + req.getServletPath());

            try {
                Thread.sleep(0);
            }
            catch (InterruptedException e) {
                log.warn("Got interrupted while annoying-sleeping, which is strange.", e);
            }

            // Check CORS
            if (!cors(req.getHeader("Origin"), resp)) return;

            // Get the Authorization header.
            String authHeader = req.getHeader("Authorization");
            if (authHeader == null) {
                log.warn("The PreConnectionOperation HTTP Auth-to-Cookie GET was invoked without"
                        + " 'Authorization' header.");
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }

            // ## NOTE!! ## THIS IS JUST FOR THE INTEGRATION TESTS!
            if (authHeader.contains(":fail_preConnectOperationServlet:")) {
                log.info("Asked to fail: 401 UNAUTHORIZED!");
                resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                return;
            }

            // :: Create auth Cookie
            // Allow arbitrary letters in Cookie value https://stackoverflow.com/a/31260616/39334
            String authHeaderBase64WithoutPadding = java.util.Base64.getEncoder().withoutPadding()
                    .encodeToString(authHeader.getBytes(StandardCharsets.UTF_8));
            Cookie authCookie = new Cookie(DummySessionAuthenticator.AUTHORIZATION_COOKIE_NAME,
                    authHeaderBase64WithoutPadding);
            authCookie.setMaxAge(30); // Running a tight ship (it is also cleared in the actual WebSocket handshake)
            authCookie.setHttpOnly(true); // No need for JavaScript to see this
            authCookie.setSecure(req.isSecure()); // If requested over SSL, set "SSL-only".
            // Note: Could have SameSite=Strict, but Servlet API does not have it - this is no problem.
            // Note: Could set path, but would require more config in SessionAuthenticator to clear it.

            // Add the Cookie and return.
            resp.addCookie(authCookie);
            resp.setStatus(HttpServletResponse.SC_NO_CONTENT);
        }

        // Based on https://stackoverflow.com/a/20204811/39334, "EDIT 2", without the ^ and $ and length restriction.
        private String regex = "^https?://(?<domain>((?!-)[a-zA-Z0-9-]{0,62}[a-zA-Z0-9]\\.)*[a-zA-Z]{2,63})(?<port>:\\d{1,5})?$";
        private Pattern pattern = Pattern.compile(regex);

        @Override
        protected void doOptions(HttpServletRequest req, HttpServletResponse resp) {
            log.info("PreConnectOperation - OPTIONS: Authorization header: "
                    + (null != req.getHeader("Authorization") ? "present" : "NOT present!")
                    + ", Origin: " + req.getHeader("Origin")
                    + ", path: " + req.getContextPath() + req.getServletPath());
            // Check CORS
            if (!cors(req.getHeader("Origin"), resp)) return;

            // Ok, return
            resp.setStatus(HttpServletResponse.SC_NO_CONTENT);
        }

        private boolean cors(String originHeader, HttpServletResponse resp) {
            // ?: Do we have an Origin header, indicating that web browser feels this is a CORS request?
            if (originHeader == null) {
                // -> No, no Origin header, so act normal, just add a little header to point out that we evaluated it.
                resp.addHeader("X-MatsSocketServer-CORS", "NoOriginHeaderInRequest_Ok");
                return true;
            }
            Matcher matches = pattern.matcher(originHeader);
            boolean match = matches.matches();
            if (!match) {
                resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
                return false;
            }
            String domain = matches.group("domain");
            String port = matches.group("port");

            boolean ok = domain.equals("localhost")
                    || domain.endsWith("stolsvik.com")
                    || domain.endsWith("stolsvik.net");
            if (!ok) {
                resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
                return false;
            }

            // # Yeah, we allow this Origin
            resp.addHeader("Access-Control-Allow-Origin", originHeader);
            // # Need to add "Vary" header when the Origin varies.. God knows..
            resp.addHeader("Vary", "Origin");
            // # Cookies and auth are allowed as headers
            resp.addHeader("Access-Control-Allow-Credentials", "true");
            // # .. specifically, the "Authorization" header
            resp.addHeader("Access-Control-Allow-Headers", "authorization");
            // NOTICE: For production: When you get things to work, you can add this header.
            // # This response can be cached for quite some time - i.e. don't do OPTIONS for next requests.
            // resp.addHeader("Access-Control-Max-Age", "86400"); // 24 hours, might be capped by browser.
            return true;
        }
    }

    /**
     * Servlet that handles out-of-band close_session, which is invoked upon window.onunload using window.sendBeacon.
     * The idea is to get the MatsSocket Session closed even if the WebSocket channel is closed at the time.
     */
    @WebServlet(WEBSOCKET_PATH + "/close_session")
    public static class OutOfBandCloseSessionServlet extends HttpServlet {
        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String sessonId = req.getParameter("session_id");
            MatsSocketServer matsSocketServer = (MatsSocketServer) req.getServletContext().getAttribute(
                    MatsSocketServer.class.getName());
            matsSocketServer.closeSession(sessonId, "Out-of-band close_session");
        }
    }

    /**
     * Servlet to supply the MatsSocket.js and test files - this only works in development (i.e. running this class from
     * e.g. IntelliJ).
     */
    @WebServlet("/mats/*")
    public static class MatsSocketLibServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String dir = (String) req.getServletContext().getAttribute(
                    CONTEXT_ATTRIBUTE_JAVASCRIPT_PATH);

            if (dir == null) {
                resp.sendError(501, "Cannot find the path for JavaScript (path not existing)"
                        + " - this only works when in development.");
                return;
            }

            Path pathWithFile = Paths.get(dir, req.getPathInfo());
            if (!Files.exists(pathWithFile)) {
                resp.sendError(501, "Cannot find the '" + req.getPathInfo()
                        + "' file (file not found) - this only works when in development.");
                return;
            }
            log.info("Path for [" + req.getPathInfo() + "]: " + pathWithFile);

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
     * Servlet to shut down this JVM (<code>System.exit(0)</code>). Employed from the Gradle integration tests.
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

        // :: Find the path to the JavaScript files (JS tests and MatsSocket.js), to provide them via Servlet.
        String pathToClasses = classesLocation.getPath();
        // .. strip down to the 'mats-websockets' path (i.e. this subproject).
        int pos = pathToClasses.indexOf("mats-websockets");
        String pathToJavaScripts = pos == -1
                ? null
                : pathToClasses.substring(0, pos) + "mats-websockets/client/javascript";
        webAppContext.getServletContext().setAttribute(CONTEXT_ATTRIBUTE_JAVASCRIPT_PATH, pathToJavaScripts);

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
