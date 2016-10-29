package com.jivesoftware.os.miru.bot.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.routing.bird.health.HealthCheck;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponse;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponseImpl;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckConfig;
import org.merlin.config.defaults.StringDefault;

import java.util.List;

class MiruBotHealthCheck implements HealthCheck {

    private List<MiruBotHealthPercent> miruBotHealthPercents = Lists.newArrayList();

    interface MiruBotHealthCheckConfig extends HealthCheckConfig {

        @Override
        @StringDefault("mirubot>fault>count")
        String getName();

        @Override
        @StringDefault("Number of miru bot faults.")
        String getDescription();

    }

    private final MiruBotHealthCheckConfig config;

    MiruBotHealthCheck(MiruBotHealthCheckConfig config) {
        this.config = config;
    }

    public HealthCheckResponse checkHealth() throws Exception {
        StringBuilder description = new StringBuilder();
        double total = 0.0;
        for (MiruBotHealthPercent miruBotHealthPercent : miruBotHealthPercents) {
            double percent = miruBotHealthPercent.getHealthPercentage();
            String desc = miruBotHealthPercent.getHealthDescription();

            if (percent > 0.0) total += percent;
            if (!desc.isEmpty()) {
                if (description.length() > 0) description.append("; ");
                description.append(desc);
            }
        }
        double health = total / miruBotHealthPercents.size();

        if (health == 1.0) {
            return new HealthCheckResponseImpl(config.getName(), 1.0, "Healthy", config.getDescription(), "", System.currentTimeMillis());
        } else {
            return new HealthCheckResponse() {
                @Override
                public String getName() {
                    return config.getName();
                }

                @Override
                public double getHealth() {
                    return health;
                }

                @Override
                public String getStatus() {
                    return "Invalid mirubot values.";
                }

                @Override
                public String getDescription() {
                    return "Invalid distinct values read from miru by mirubot.";
                }

                @Override
                public String getResolution() {
                    return "Investigate invalid values: " + description.toString();
                }

                @Override
                public long getTimestamp() {
                    return System.currentTimeMillis();
                }
            };
        }
    }

    void addServiceHealth(MiruBotHealthPercent miruBotHealthPercent) {
        miruBotHealthPercents.add(miruBotHealthPercent);
    }

}
