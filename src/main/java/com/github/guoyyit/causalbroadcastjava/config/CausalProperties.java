package com.github.guoyyit.causalbroadcastjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration knobs for the causal broadcast service.
 */
@ConfigurationProperties(prefix = "causal")
public class CausalProperties {

    private String dataDir = "./data";

    private int gcBatchSize = 100;

    private int pullMaxLimit = 500;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public int getGcBatchSize() {
        return gcBatchSize;
    }

    public void setGcBatchSize(int gcBatchSize) {
        this.gcBatchSize = gcBatchSize;
    }

    public int getPullMaxLimit() {
        return pullMaxLimit;
    }

    public void setPullMaxLimit(int pullMaxLimit) {
        this.pullMaxLimit = pullMaxLimit;
    }
}
