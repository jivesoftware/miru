package com.jivesoftware.os.miru.plugin;

import com.jivesoftware.os.miru.api.base.MiruIBA;
import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

/**
 *
 * @author jonathan.colt
 */
public abstract class MiruInterner<T extends MiruIBA> {

    public abstract T create(byte[] bytes);

    private final WeakHashMap<T, WeakReference<T>>[] pools;
    private final MiruIBA[] keys;
    private final boolean enabled;

    public MiruInterner(boolean enabled) {
        int concurencyLevel = 1024;
        this.keys = new MiruIBA[concurencyLevel]; // TODO config
        for (int i = 0; i < keys.length; i++) {
            keys[i] = new MiruIBA(new byte[0]);
        }
        this.pools = new WeakHashMap[concurencyLevel];
        for (int i = 0; i < pools.length; i++) {
            pools[i] = new WeakHashMap<>();
        }
        this.enabled = enabled;
    }

    public T intern(byte[] bytes) {
        if (!enabled) {
            return create(bytes);
        } else {
            return doIntern(bytes, 0, bytes.length);
        }
    }

    public T intern(byte[] bytes, int offset, int length) {
        if (!enabled) {
            byte[] exactBytes = new byte[length];
            System.arraycopy(bytes, offset, exactBytes, 0, length);
            return create(bytes);
        }
        return doIntern(bytes, offset, length);
    }

    // He likes to watch.
    private T doIntern(byte[] bytes, int offset, int length) {
        int hashCode = MiruIBA.hashCode(bytes, offset, length);
        int index = Math.abs(hashCode % keys.length);
        MiruIBA key = keys[index];
        WeakHashMap<T, WeakReference<T>> pool = pools[index];
        synchronized (pool) {
            byte[] exactBytes = new byte[length];
            System.arraycopy(bytes, offset, exactBytes, 0, length);
            key.mutate(exactBytes, hashCode);

            T res;
            WeakReference<T> ref = pool.get(key);
            if (ref != null) {
                res = ref.get();
            } else {
                res = null;
            }
            if (res == null) {
                res = create(exactBytes);
                pool.put(res, new WeakReference<>(res));
            }
            return res;
        }
    }
}
