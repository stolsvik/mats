package com.stolsvik.mats.impl.jms;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;

public class JmsMatsContextLocalCallback implements BiFunction<Class<?>, String[], Object> {
    private static final ThreadLocal<Map<Object, Object>> THREAD_LOCAL_MAP = ThreadLocal.withInitial(HashMap::new);

    /**
     * <b>Note: These bind/unbind methods are not for Mats API users, but Mats API implementors.</b>
     */
    public static void bindResource(Object key, Object value) {
        THREAD_LOCAL_MAP.get().put(key, value);
    }

    /**
     * <b>Note: These bind/unbind methods are not for Mats API users, but Mats API implementors.</b>
     */
    public static void unbindResource(Object key) {
        THREAD_LOCAL_MAP.get().remove(key);
    }

    @Override
    public Object apply(Class<?> type, String[] name) {
        // :: First check whether we're inside a stage or initiate context, and can find it in
        // ProcessContext or MatsInitiate
        // FIRST check MatsInitiate (initiate context), as it is POSSIBLE to do an initiate from within Stage
        MatsInitiate init = (MatsInitiate) THREAD_LOCAL_MAP.get().get(MatsInitiate.class);
        // ?: Did we find the MatsInitiate?
        if (init != null) {
            // -> Yes, so forward the call to MatsInitiate
            Optional<?> optionalT = init.getAttribute(type, name);
            if (optionalT.isPresent()) {
                return optionalT;
            }
        }

        // E-> THEN check ProcessContext (stage context)
        ProcessContext<?> ctx = (ProcessContext<?>) THREAD_LOCAL_MAP.get().get(ProcessContext.class);
        // ?: Did we find the ProcessContext?
        if (ctx != null) {
            // -> Yes, so forward the call to ProcessContext
            Optional<?> optionalT = ctx.getAttribute(type, name);
            if (optionalT.isPresent()) {
                return optionalT;
            }
        }

        // E-> No, check by name.
        if (name.length != 0) {
            String joinedKey = String.join(".", name);
            Object keyedObject = THREAD_LOCAL_MAP.get().get(joinedKey);
            if ((keyedObject != null)) {
                if (!type.isInstance(keyedObject)) {
                    throw new ClassCastException("The Object stored on key [" + joinedKey + "] is not of that type!");
                }
                return Optional.of(cast(keyedObject));
            }
        }

        // E-> No, check direct lookup of type in the ThreadLocal map.
        Object direct = THREAD_LOCAL_MAP.get().get(type);
        if (direct != null) {
            if (!(type.isInstance(direct))) {
                throw new ClassCastException("The Object stored on key [" + type + "] is not of that type!");
            }
            return Optional.of(cast(direct));
        }

        // E-> No luck.
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object object) {
        return (T) object;
    }
}
