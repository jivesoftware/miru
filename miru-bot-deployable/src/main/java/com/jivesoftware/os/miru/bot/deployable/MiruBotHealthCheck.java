package com.jivesoftware.os.miru.bot.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.routing.bird.health.HealthCheck;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponse;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponseImpl;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckConfig;
import org.merlin.config.defaults.StringDefault;

import java.util.List;

class MiruBotHealthCheck implements HealthCheck {

    interface MiruBotHealthCheckConfig extends HealthCheckConfig {

        @Override
        @StringDefault("mirubot>fault>count")
        String getName();

        @Override
        @StringDefault("Number of miru bot faults.")
        String getDescription();

    }

    private final MiruBotHealthCheckConfig config;

    private List<MiruBotHealthPercent> miruBotHealthPercentList = Lists.newArrayList();

    MiruBotHealthCheck(MiruBotHealthCheckConfig config) {
        this.config = config;
    }

    void addMiruBotHealthPercenter(MiruBotHealthPercent miruBotHealthPercent) {
        miruBotHealthPercentList.add(miruBotHealthPercent);
    }

    public HealthCheckResponse checkHealth() throws Exception {
        final double PERFECT_HEALTH = 1.0;
        double[] healthPercentage = new double[]{PERFECT_HEALTH};

        StringBuilder healthDescription = new StringBuilder();
        for (MiruBotHealthPercent miruBotHealthPercent : miruBotHealthPercentList) {
            double health = miruBotHealthPercent.getHealthPercentage();
            if (health < PERFECT_HEALTH) {
                healthPercentage[0] = Math.min(healthPercentage[0], health);

                String description = miruBotHealthPercent.getHealthDescription();
                if (!description.isEmpty()) {
                    if (healthDescription.length() > 0) healthDescription.append("; ");
                    healthDescription.append(description);
                }
            }
        }

        if (healthPercentage[0] == PERFECT_HEALTH) {
            return new HealthCheckResponseImpl(
                    config.getName(),
                    PERFECT_HEALTH,
                    "Healthy",
                    config.getDescription(),
                    "",
                    System.currentTimeMillis());
        } else {
            return new HealthCheckResponse() {
                @Override
                public String getName() {
                    return config.getName();
                }

                @Override
                public double getHealth() {
                    return healthPercentage[0];
                }

                @Override
                public String getStatus() {
                    return "Invalid mirubot values.";
                }

                @Override
                public String getDescription() {
                    return "Invalid values read from miru by mirubot.";
                }

                @Override
                public String getResolution() {
                    return "Investigate invalid values: " + healthDescription.toString();
                }

                @Override
                public long getTimestamp() {
                    return System.currentTimeMillis();
                }
            };
        }
    }

}
