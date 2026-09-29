package com.github.guoyyit.causalbroadcastjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the causal broadcast service.
 */
@ConfigurationProperties(prefix = "causal.broadcast")
public class CausalBroadcastProperties {

    /** Directory where persistent state is stored. */
    private String dataDir = "./data";

    /** Whether broadcast automatically fans out to all active participants. */
    private boolean autoFanout = true;

    /** Maximum number of delivered messages retained behind the read cursor. */
    private int deliveredRetention = 0;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public boolean isAutoFanout() {
        return autoFanout;
    }

    public void setAutoFanout(boolean autoFanout) {
        this.autoFanout = autoFanout;
    }

    public int getDeliveredRetention() {
        return deliveredRetention;
    }

    public void setDeliveredRetention(int deliveredRetention) {
        this.deliveredRetention = deliveredRetention;
    }
}
