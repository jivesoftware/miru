package com.jivesoftware.os.miru.plugin.query;

import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.ClientHealth;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import com.jivesoftware.os.routing.bird.shared.ReturnFirstNonFailure.Favored;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jonathan.colt on 5/5/17.
 */
final class InterceptingNextClientStrategy implements NextClientStrategy {
    public final NextClientStrategy delegate;
    public int attempt;
    public int totalAttempts;
    public ConnectionDescriptor[] connectionDescriptors;
    public ConnectionDescriptor favoredConnectionDescriptor;
    public long latency;

    InterceptingNextClientStrategy(NextClientStrategy delegate) {
        this.delegate = delegate;
    }

    @Override
    public <C, R> R call(String family,
        ClientCall<C, R, HttpClientException> httpCall,
        ConnectionDescriptor[] connectionDescriptors,
        long connectionDescriptorsVersion,
        C[] clients,
        ClientHealth[] clientHealths,
        int deadAfterNErrors,
        long checkDeadEveryNMillis,
        AtomicInteger[] clientsErrors,
        AtomicLong[] clientsDeathTimestamp,
        Favored favored) throws HttpClientException {

        this.connectionDescriptors = connectionDescriptors;

        return delegate.call(family, httpCall, connectionDescriptors, connectionDescriptorsVersion, clients, clientHealths, deadAfterNErrors,
            checkDeadEveryNMillis, clientsErrors, clientsDeathTimestamp, (attempt, totalAttempts, favoredConnectionDescriptor, latency) -> {
                this.attempt = attempt;
                this.totalAttempts = totalAttempts;
                this.favoredConnectionDescriptor = favoredConnectionDescriptor;
                this.latency = latency;
            });
    }
}
