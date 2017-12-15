package com.jivesoftware.data.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.data.DBaaSConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public class PasswordManager {

    private final static Logger LOG = LoggerFactory.getLogger(PasswordManager.class);

    private SecureRandom random = new SecureRandom();

    private final Map<String,Instance> instanceMap;

    @Inject
    public PasswordManager(DBaaSConfiguration dBaaSConfiguration) {
        this.instanceMap = load(dBaaSConfiguration.getPasswordFile());
    }

    public Optional<Instance> getInstance(String instanceId) {
        return Optional.ofNullable(instanceMap.get(instanceId));
    }

    public String generatePassword() {
        return new BigInteger(130, random).toString(32);
    }

    /**
     * Loads the configuration.
     */
    public Map<String,Instance> load(String passwordFile) {

        Instances instances;
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            instances = mapper.readValue(new File(passwordFile),
                    Instances.class);
        }
        catch (Exception e) {
            LOG.error(String.format("Error reading instance key file %s", passwordFile), e);
            return ImmutableMap.of();
        }

        return ImmutableMap.copyOf(instances.instances.stream().collect(toMap(k -> k.getInstanceId(),
                Function.identity())));
    }

    @SuppressWarnings("unused")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Instances {

        private List<Instance> instances;

        @JsonProperty
        public List<Instance> getInstances() {
            return instances;
        }
    }

    @SuppressWarnings("unused")
    public static class Instance {

        private String instanceId;
        private String username;
        private String password;

        @JsonProperty
        public String getInstanceId() {
            return instanceId;
        }

        @JsonProperty
        public String getUsername() {
            return username;
        }

        @JsonProperty
        public String getPassword() {
            return password;
        }
    }

}
