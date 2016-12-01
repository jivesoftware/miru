package com.jivesoftware.os.miru.sync.deployable.oauth;

import com.google.common.collect.Maps;
import com.jivesoftware.os.routing.bird.server.oauth.AuthValidationException;
import com.jivesoftware.os.routing.bird.server.oauth.OAuthSecretManager;
import com.jivesoftware.os.routing.bird.server.oauth.validator.AuthValidator;
import com.jivesoftware.os.routing.bird.server.oauth.validator.DefaultOAuthValidator;
import com.jivesoftware.os.routing.bird.server.oauth.validator.DryRunOAuthValidator;
import com.jivesoftware.os.routing.bird.server.oauth.validator.NoOpAuthValidator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.glassfish.jersey.oauth1.signature.OAuth1Request;
import org.glassfish.jersey.oauth1.signature.OAuth1Signature;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class MiruSyncOAuthValidatorInitializer {

    public interface MiruSyncOAuthValidatorConfig extends Config {

        @BooleanDefault(true)
        boolean getOauthValidatorIsEnabled();

        @BooleanDefault(false)
        boolean getOauthValidatorIsDryRun();

        @BooleanDefault(true)
        boolean getOauthValidatorLoadBalancerRejiggeringEnabled();

        @BooleanDefault(true)
        boolean getOauthValidatorLoadBalancerPortRejiggeringEnabled();

        @LongDefault(60 * 1000)
        long getOauthValidatorRequestTimestampAgeLimitMillis();

        @StringDefault("")
        String getOauthConsumerKeyTokenTuples();
    }

    public AuthValidator<OAuth1Signature, OAuth1Request> initialize(MiruSyncOAuthValidatorConfig config) throws Exception {

        if (!config.getOauthValidatorIsEnabled()) {
            return (AuthValidator) NoOpAuthValidator.SINGLETON;
        }

        Map<String, String> consumerKeyTokens = Maps.newConcurrentMap();
        String tuples = config.getOauthConsumerKeyTokenTuples();
        if (tuples != null) {
            String[] keyTokenPairs = tuples.trim().split("\\s*,\\s*");
            for (String pair : keyTokenPairs) {
                String[] parts = pair.trim().split(":");
                consumerKeyTokens.put(parts[0], parts[1]);
            }
        }

        OAuthSecretManager authSecretManager = new OAuthSecretManager() {
            @Override
            public void clearCache() {
            }

            @Override
            public String getSecret(String s) throws AuthValidationException {
                return consumerKeyTokens.get(s);
            }

            @Override
            public void verifyLastSecretRemovalTime() throws Exception {
            }
        };

        AuthValidator<OAuth1Signature, OAuth1Request> oAuthValidator = new DefaultOAuthValidator(Executors.newScheduledThreadPool(1),
            TimeUnit.DAYS.toMillis(1),
            authSecretManager,
            config.getOauthValidatorRequestTimestampAgeLimitMillis(),
            config.getOauthValidatorLoadBalancerRejiggeringEnabled(),
            config.getOauthValidatorLoadBalancerPortRejiggeringEnabled());

        if (config.getOauthValidatorIsDryRun()) {
            oAuthValidator = new DryRunOAuthValidator(oAuthValidator);
        }

        return oAuthValidator;
    }
}
