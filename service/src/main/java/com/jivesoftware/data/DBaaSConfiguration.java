package com.jivesoftware.data;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import java.util.List;

@SuppressWarnings("unused")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DBaaSConfiguration extends Configuration {

    private String instanceIdentifierPrefix;
    private String awsAccountNumber;
    private Regions awsRegion;
    private String managedServiceTag;
    private String jiveServiceTag;
    private String serviceComponentTag;
    private String pipelinePhase;
    private long hardDeleteDelay;
    private String sharedInstanceDeployColor;
    private QueueConfig creationQueue;
    private QueueConfig deletionQueue;
    private Integer enhancedMetricsTiming;
    private Integer metricsPolling;
    private String passwordFile;
    private String tokenFile;
    private String aesFile;
    private String makoEnvironment;
    private DomainDetails domainDetails;
    private Integer queuePollFrequencySeconds;
    private DatabaseDaoConfiguration databaseDaoConfiguration;
    private CloneConfiguration cloneConfiguration;
    private InstanceTemplate instanceTemplate;
    private List<String> defaultInstances;
    private List<InstanceType> instanceTypes;
    private String defaultInstanceClass;
    private Integer defaultInstanceStorage;
    private String defaultDBName;

    private List<QueueConfig> queueConfigs;

    @JsonProperty
    public String getInstanceIdentifierPrefix() {
        return instanceIdentifierPrefix;
    }

    @JsonProperty
    public String getAwsAccountNumber() {
        return awsAccountNumber;
    }

    @JsonProperty
    public Region getAwsRegion() {
        return Region.getRegion(awsRegion);
    }

    @JsonProperty
    public String getManagedServiceTag() {
        return managedServiceTag;
    }

    @JsonProperty
    public String getJiveServiceTag() {
        return jiveServiceTag;
    }

    @JsonProperty
    public String getServiceComponentTag() {
        return serviceComponentTag;
    }

    @JsonProperty
    public long getHardDeleteDelay() { return hardDeleteDelay; }

    @JsonProperty
    public String getSharedInstanceDeployColor() { return sharedInstanceDeployColor; }

    @JsonProperty
    public QueueConfig getCreationQueue() { return creationQueue; }

    @JsonProperty
    public QueueConfig getDeletionQueue() { return deletionQueue; }

    @JsonProperty
    public Integer getEnhancedMetricsTiming() { return enhancedMetricsTiming; }

    @JsonProperty
    public Integer getMetricsPolling() { return metricsPolling; }

    @JsonProperty
    public String getPasswordFile() {
        return passwordFile;
    }

    @JsonProperty
    public String getTokenFile() {
        return tokenFile;
    }

    @JsonProperty
    public String getAesFile() { return aesFile; }

    @JsonProperty
    public String getMakoEnvironment() { return makoEnvironment; }

    @JsonProperty
    public DomainDetails getDomainDetails() { return domainDetails; }

    @JsonProperty
    public DatabaseDaoConfiguration getDatabaseDaoConfiguration() {
        return databaseDaoConfiguration;
    }

    @JsonProperty
    public CloneConfiguration getCloneConfiguration() {
        return cloneConfiguration;
    }

    @JsonProperty
    public InstanceTemplate getInstanceTemplate() {
        return instanceTemplate;
    }

    @JsonProperty
    public List<String> getDefaultInstances() {
        return defaultInstances;
    }

    @JsonProperty
    public String getDefaultDBName() {
        return defaultDBName;
    }

    @JsonProperty
    public List<InstanceType> getInstanceTypes() {
        return instanceTypes;
    }

    public static class InstanceTemplate {

        private Integer allocatedStorage;
        private String dbInstanceClass;
        private String engine;
        private boolean multiAZ;
        private Integer port;
        private Boolean publiclyAccessible;
        private String subnetGroup;
        private String securityGroup;
        private String engineVersion;
        private String masterUser;
        private String dbName;

        public String getDbInstanceClass() {
            return dbInstanceClass;
        }

        public String getEngine() {
            return engine;
        }

        public boolean isMultiAZ() {
            return multiAZ;
        }

        public Integer getPort() {
            return port;
        }

        public Boolean isPubliclyAccessible() {
            return publiclyAccessible;
        }

        public String getSubnetGroup() {
            return subnetGroup;
        }

        public String getSecurityGroup() { return securityGroup; }

        public String getEngineVersion() { return engineVersion; }

        public Integer getAllocatedStorage() {
            return allocatedStorage;
        }

        public String getMasterUser() {
            return masterUser;
        }

        public String getDbName() {
            return dbName;
        }
    }

    public static class DatabaseDaoConfiguration {

        private String tableName;
        private Long readUnits;
        private Long writeUnits;

        public String getTableName() {
            return tableName;
        }

        public Long getReadUnits() {
            return readUnits;
        }

        public Long getWriteUnits() {
            return writeUnits;
        }
    }

    public static class CloneConfiguration {

        private String command;
        private Long executionTimeout;

        public Long getExecutionTimeout() {
            return executionTimeout;
        }

        public String getCommand() {
            return command;
        }
    }

    public static class QueueConfig {

        private String queueName;
        private Integer queuePollFrequencySeconds;

        public String getQueueName() { return queueName; }

        public Integer getQueuePollFrequencySeconds() { return queuePollFrequencySeconds; }
    }

    public static class DomainDetails {

        private String domainName;
        private String protocol;

        public String getDomainName() { return domainName; }

        public String getProtocol() { return protocol; }
    }

    public static class InstanceType {

        @JsonProperty
        private String instanceClass;
        private int totalRam;
        private int totalCores;
        private boolean prodApproved;
        private boolean encryptedAvailable;
        private boolean multiAZAvailable;

        public InstanceType(String instanceClass,
                            int totalRam,
                            int totalCores,
                            boolean prodApproved,
                            boolean encryptedAvailable,
                            boolean multiAZAvailable){
            this.instanceClass=instanceClass;
            this.totalRam=totalRam;
            this.totalCores=totalCores;
            this.prodApproved=prodApproved;
            this.encryptedAvailable=encryptedAvailable;
            this.multiAZAvailable=multiAZAvailable;
        }

        public InstanceType(){}

        public String getInstanceClass() {return instanceClass;}

        public int getTotalRam() {return totalRam;}

        public int getTotalCores() {return totalCores;}

        public boolean getProdApproved() {return prodApproved;}

        public boolean getEncryptedAvailable() {return encryptedAvailable;}

        public boolean getMultiAZAvailable() {return multiAZAvailable;}

    }
}
