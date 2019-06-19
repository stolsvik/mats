package com.stolsvik.mats.spring;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.MatsFactoryWrapper;

/**
 * A wrapper which {@link MatsFactory}s in a Spring context should be wrapped in. If you use the MatsFactories created
 * by Mats' Spring-specific utility classes, they will already be wrapped.
 * <p>
 * Currently (as of 2019-06-10), it doesn't really matter if a MatsFactory is wrapped or not, but with an eye towards
 * future-proofing, it is great if they all are. Therefore, @EnableMats will complain using a WARN logline if it
 * encounters a non-wrapped MatsFactory.
 *
 * @author Endre Stølsvik 2019-06-10 14:19 - http://stolsvik.com/, endre@stolsvik.com
 */
public class SpringMatsFactory extends MatsFactoryWrapper implements BeanNameAware {

    // Use clogging, since that's what Spring does.
    private static final Log log = LogFactory.getLog(SpringMatsFactory.class);

    private final MatsFactorySupplier _targetMatsFactorySupplier;

    public static SpringMatsFactory wrapTargetMatsFactory(MatsFactorySupplier targetMatsFactorySupplier) {
        return new SpringMatsFactory(targetMatsFactorySupplier);
    }

    private SpringMatsFactory(MatsFactorySupplier targetMatsFactorySupplier) {
        log.debug("Instantiating SpringMatsFactory, with MatsFactorySupplier ["+targetMatsFactorySupplier+"].");
        _targetMatsFactorySupplier = targetMatsFactorySupplier;
        fetchTargetMatsFactory();
    }

    @FunctionalInterface
    public interface MatsFactorySupplier {
        MatsFactory get() throws Exception;
    }

    protected MatsFactory _targetMatsFactory;

    protected void fetchTargetMatsFactory() {
        try {
            _targetMatsFactory = _targetMatsFactorySupplier.get();
        }
        catch (Exception e) {
            throw new MatsFactoryCouldNotBeCreatedException("Couldn't get MatsFactory from supplier ["
                    + _targetMatsFactorySupplier + "].", e);
        }
    }

    @Override
    public void setBeanName(String name) {
        getTargetMatsFactory().getFactoryConfig().setName(name);
    }

    @Override
    public MatsFactory getTargetMatsFactory() {
        return _targetMatsFactory;
    }

    @Override
    public void setTargetMatsFactory(MatsFactory targetMatsFactory) {
        throw new IllegalStateException("You cannot set a target MatsFactory on a " + this.getClass().getSimpleName()
                + "; A MatsFactorySupplier must be provided in the constructor.");
    }

    private static class MatsFactoryCouldNotBeCreatedException extends RuntimeException {
        public MatsFactoryCouldNotBeCreatedException(String msg, Exception e) {
            super(msg, e);
        }
    }
}
