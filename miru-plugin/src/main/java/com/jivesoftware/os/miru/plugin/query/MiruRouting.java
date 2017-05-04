package com.jivesoftware.os.miru.plugin.query;

import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;

/**
 * Created by jonathan.colt on 5/4/17.
 */
public interface MiruRouting {

    <Q, A> MiruResponse<A> query(String routingTenant,
        String family,
        MiruRequest<Q> request,
        String path,
        Class<A> answerClass) throws Exception;
}
