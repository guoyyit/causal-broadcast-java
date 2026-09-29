package com.github.guoyyit.causalbroadcastjava.config;

import com.github.guoyyit.causalbroadcastjava.persistence.StateStore;
import com.github.guoyyit.causalbroadcastjava.service.CausalBroadcastService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * Wires the causal broadcast service and its persistent store.
 */
@Configuration
@EnableConfigurationProperties(CausalBroadcastProperties.class)
public class CausalBroadcastConfig {

    @Bean
    public StateStore stateStore(CausalBroadcastProperties properties) {
        return new StateStore(Path.of(properties.getDataDir()));
    }

    @Bean
    public CausalBroadcastService causalBroadcastService(StateStore stateStore,
                                                         CausalBroadcastProperties properties) {
        return new CausalBroadcastService(stateStore, properties.isAutoFanout(),
                properties.getDeliveredRetention());
    }
}
