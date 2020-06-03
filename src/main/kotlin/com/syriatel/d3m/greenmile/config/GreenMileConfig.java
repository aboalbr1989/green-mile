package com.syriatel.d3m.greenmile.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.util.unit.DataSize;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "green-mile.config")
public class GreenMileConfig {
    private final KafkaProperties.Ssl ssl = new KafkaProperties.Ssl();
    private String applicationId;
    private boolean autoStartup = true;
    private List<String> bootstrapServers;
    private DataSize cacheMaxSizeBuffering;
    private String clientId;
    private Integer replicationFactor;
    private String stateDir;
    private final Map<String, String> properties = new HashMap<>();


    public KafkaProperties.Ssl getSsl() {
        return this.ssl;
    }

    public String getApplicationId() {
        return this.applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public boolean isAutoStartup() {
        return this.autoStartup;
    }

    public void setAutoStartup(boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    public List<String> getBootstrapServers() {
        return this.bootstrapServers;
    }

    public void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public DataSize getCacheMaxSizeBuffering() {
        return this.cacheMaxSizeBuffering;
    }

    public void setCacheMaxSizeBuffering(DataSize cacheMaxSizeBuffering) {
        this.cacheMaxSizeBuffering = cacheMaxSizeBuffering;
    }

    public String getClientId() {
        return this.clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Integer getReplicationFactor() {
        return this.replicationFactor;
    }

    public void setReplicationFactor(Integer replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public String getStateDir() {
        return this.stateDir;
    }

    public void setStateDir(String stateDir) {
        this.stateDir = stateDir;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public Map<String, Object> buildProperties() {
        Properties properties = new Properties();
        PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
        map.from(this::getApplicationId).to(properties.in("application.id"));
        map.from(this::getBootstrapServers).to(properties.in("bootstrap.servers"));
        map.from(this::getCacheMaxSizeBuffering).asInt(DataSize::toBytes).to(properties.in("cache.max.bytes.buffering"));
        map.from(this::getClientId).to(properties.in("client.id"));
        map.from(this::getReplicationFactor).to(properties.in("replication.factor"));
        map.from(this::getStateDir).to(properties.in("state.dir"));
        return properties.with(this.ssl, this.properties);
    }

    private static class Properties extends HashMap<String, Object> {

        <V> java.util.function.Consumer<V> in(String key) {
            return (value) -> put(key, value);
        }

        Properties with(KafkaProperties.Ssl ssl, Map<String, String> properties) {
            putAll(ssl.buildProperties());
            putAll(properties);
            return this;
        }

    }
}
