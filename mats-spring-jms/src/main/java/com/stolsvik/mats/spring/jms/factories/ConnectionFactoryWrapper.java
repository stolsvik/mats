package com.stolsvik.mats.spring.jms.factories;

import com.stolsvik.mats.MatsFactory.MatsWrapper;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

/**
 * A base Wrapper for {@link ConnectionFactory}, which simply implements ConnectionFactory, takes a ConnectionFactory
 * instance and forwards all calls to that. Meant to be extended to add extra functionality, e.g. Spring integration.
 *
 * @author Endre Stølsvik 2019-06-10 11:43 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ConnectionFactoryWrapper implements MatsWrapper<ConnectionFactory>, ConnectionFactory {

    /**
     * This field is private - if you in extensions need the instance, invoke {@link #getTarget()}. If
     * you want to take control of the wrapped ConnectionFactory instance, then override
     * {@link #getTarget()}.
     */
    private ConnectionFactory _targetConnectionFactory;

    /**
     * Standard constructor, taking the wrapped {@link ConnectionFactory} instance.
     *
     * @param targetConnectionFactory
     *            the {@link ConnectionFactory} instance which {@link #getTarget()} will return (and
     *            hence all forwarded methods will use).
     */
    public ConnectionFactoryWrapper(ConnectionFactory targetConnectionFactory) {
        setTarget(targetConnectionFactory);
    }

    /**
     * No-args constructor, which implies that you either need to invoke
     * {@link #setTarget(ConnectionFactory)} before publishing the instance (making it available for
     * other threads), or override {@link #getTarget()} to provide the desired
     * {@link ConnectionFactory} instance. In these cases, make sure to honor memory visibility semantics - i.e.
     * establish a happens-before edge between the setting of the instance and any other threads getting it.
     */
    public ConnectionFactoryWrapper() {
        /* no-op */
    }

    /**
     * Sets the wrapped {@link ConnectionFactory}, e.g. in case you instantiated it with the no-args constructor. <b>Do
     * note that the field holding the wrapped instance is not volatile nor synchronized</b>. This means that if you
     * want to set it after it has been published to other threads, you will have to override both this method and
     * {@link #getTarget()} to provide for needed memory visibility semantics, i.e. establish a
     * happens-before edge between the setting of the instance and any other threads getting it.
     *
     * @param targetConnectionFactory
     *            the {@link ConnectionFactory} which is returned by {@link #getTarget()}, unless that
     *            is overridden.
     */
    public void setTarget(ConnectionFactory targetConnectionFactory) {
        _targetConnectionFactory = targetConnectionFactory;
    }

    /**
     * @return the wrapped {@link ConnectionFactory}. All forwarding methods invokes this method to get the wrapped
     *         {@link ConnectionFactory}, thus if you want to get creative wrt. how and when the ConnectionFactory is
     *         decided, you can override this method.
     */
    public ConnectionFactory getTarget() {
        if (_targetConnectionFactory == null) {
            throw new IllegalStateException("ConnectionFactoryWrapper.getTarget():"
                    + " The target ConnectionFactory is not set!");
        }
        return _targetConnectionFactory;
    }

    /**
     * @deprecated #setTarget
     */
    @Deprecated
    public void setTargetConnectionFactory(ConnectionFactory targetConnectionFactory) {
        setTarget(targetConnectionFactory);
    }

    /**
     * @deprecated #getTarget
     */
    @Deprecated
    public ConnectionFactory getTargetConnectionFactory() {
        return getTarget();
    }

    @Override
    public Connection createConnection() throws JMSException {
        return getTarget().createConnection();
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return getTarget().createConnection(userName, password);
    }
}
