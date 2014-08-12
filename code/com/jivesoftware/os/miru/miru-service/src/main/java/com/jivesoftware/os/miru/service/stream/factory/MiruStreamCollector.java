package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;

/**
 *
 * @param <R>
 */
public interface MiruStreamCollector<R> {

    R collect(Optional<R> result) throws Exception;

}
