package com.jivesoftware.data.impl;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.*;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.*;
import com.jivesoftware.data.resources.entities.Instance;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.util.*;

import static com.codahale.metrics.MetricRegistry.name;

public class InstanceManager {

    private static final String IDENTIFIER_FORMAT = "%s-%s";
    private static final String SERVICE_TAG_KEY = "jive_service";
    private static final String SERVICE_COMPONENT_TAG_KEY = "service_component";
    private static final String MANAGED_BY_SERVICE_COMPONENT_TAG_KEY = "managed_by_service";
    private static final String DEFAULT_TAG_KEY = "default_by_service";
    private static final String MAKO_ENVIRONMENT = "mako-environment";
    //private static final String CONNECTIONS_STATISTICS_NAME = "DatabaseConnections";
    private static final String CPU_STATISTICS_NAME = "CPUUtilization";
    private static final String SSD_STORAGE = "gp2";
    private static final String RDS_UNITS = "GB";
    private static final Set<String> READY_RDS_STATUS_SET = ImmutableSet.of("available",
            "modifying", "backing-up");

    private static final Set<String> SMALLCLASSES = ImmutableSet.of
            ("db.t2.micro", "db.t2.small", "db.t2.medium");

    private final static Logger logger = LoggerFactory.getLogger(InstanceManager.class);
    private final AmazonCloudWatchClient amazonCloudWatchClient;
    private final DBaaSConfiguration dBaaSConfiguration;
    private final AmazonRDSClient rdsClient;
    private final Timer findDefaultInstanceTimer;

    @Inject
    public InstanceManager(AmazonRDSClient rdsClient,
                           DBaaSConfiguration dBaaSConfiguration,
                           MetricRegistry metricRegistry,
                           AmazonCloudWatchClient amazonCloudWatchClient) {
        this.dBaaSConfiguration = dBaaSConfiguration;
        this.rdsClient = rdsClient;
        this.amazonCloudWatchClient = amazonCloudWatchClient;
        this.findDefaultInstanceTimer = metricRegistry.timer(name(InstanceManager.class,"findLeastCPUInstance"));
    }

    public DBInstance createSharedInstance(@NotNull String instanceId,
                                           @NotNull String masterUser,
                                           @NotNull String masterPassword,
                                           @NotNull String dbName) {
        return createInstance(instanceId,
                Optional.of(masterUser),
                masterPassword,
                dBaaSConfiguration.getInstanceTemplate().getDbInstanceClass(),
                dBaaSConfiguration.getInstanceTemplate().getAllocatedStorage(),
                Optional.of(dbName), true, Optional.empty(), Optional.empty());
    }

    public DBInstance createDedicatedInstance(@NotNull String instanceId,
                                              @NotNull String masterPassword,
                                              @NotNull String serviceTag,
                                              @NotNull String serviceComponentTag,
                                              @NotNull String instanceClass,
                                              @NotNull Integer instanceStorage) {
        return createInstance(instanceId,
                Optional.empty(),
                masterPassword,
                instanceClass,
                instanceStorage,
                Optional.empty(),
                false,
                Optional.of(serviceTag),
                Optional.of(serviceComponentTag));
    }


    private DBInstance createInstance(@NotNull String instanceId,
                                      @NotNull Optional<String> masterUser,
                                      @NotNull String masterPassword,
                                      @NotNull String dbInstanceClass,
                                      @NotNull Integer dbInstanceStorage,
                                      @NotNull Optional<String> dbName, boolean isDefault,
                                      @NotNull Optional<String> serviceTag,
                                      @NotNull Optional<String> serviceComponentTag) {
        DBInstance dbInstance;

        DBaaSConfiguration.InstanceTemplate instanceTemplate =
                dBaaSConfiguration.getInstanceTemplate();
        try {
            CreateDBInstanceRequest createDBInstanceRequest = new CreateDBInstanceRequest()
                    .withAllocatedStorage(dbInstanceStorage)
                    .withDBInstanceClass(dbInstanceClass)
                    .withEngine(instanceTemplate.getEngine())
                    .withMasterUsername(masterUser.orElse(instanceTemplate.getMasterUser()))
                    .withDBInstanceIdentifier(instanceId)
                    .withMasterUserPassword(masterPassword)
                    .withMultiAZ(instanceTemplate.isMultiAZ())
                    .withPubliclyAccessible(instanceTemplate.isPubliclyAccessible())
                    .withTags(getTags(serviceTag.orElse(dBaaSConfiguration.getJiveServiceTag()),
                            serviceComponentTag.orElse(dBaaSConfiguration.getServiceComponentTag()),
                            isDefault))
                    .withEngineVersion(instanceTemplate.getEngineVersion())
                    .withDBName(dbName.orElse(instanceTemplate.getDbName()))
                    .withPort(instanceTemplate.getPort())
                    .withStorageType(SSD_STORAGE)
                    .withMonitoringInterval(dBaaSConfiguration.getEnhancedMetricsTiming());

            if(!SMALLCLASSES.contains(dbInstanceClass)) {
                createDBInstanceRequest = createDBInstanceRequest.withStorageEncrypted(true);
            }

            if ("data-dbaas-ms-pipeline-group".equals(instanceTemplate.getSubnetGroup())) {
                createDBInstanceRequest.withMonitoringRoleArn("arn:aws:iam::811034720611:role/dbaas-emaccess-role");
            } else if ("data-dbaas-ms-prod-group".equals(instanceTemplate.getSubnetGroup())) {
                createDBInstanceRequest.withMonitoringRoleArn("arn:aws:iam::663559125979:role/dbaas-emaccess-role");
            } else {
                createDBInstanceRequest.withMonitoringRoleArn("arn:aws:iam::072535113705:role/dbaas-emaccess");
            }

            if (!StringUtils.isEmpty(instanceTemplate.getSubnetGroup())) {
                logger.debug(String.format("Setting subnet group to %s for instance %s",
                        instanceId, instanceTemplate.getSubnetGroup()));
                createDBInstanceRequest =
                        createDBInstanceRequest.withDBSubnetGroupName(
                                instanceTemplate.getSubnetGroup());
            }
            if (!StringUtils.isEmpty(instanceTemplate.getSecurityGroup())) {
                logger.debug(String.format("Setting security group to %s for instance %s",
                        instanceId, instanceTemplate.getSecurityGroup()));
                List<String> VPCList = ImmutableList.of(instanceTemplate.getSecurityGroup());
                createDBInstanceRequest =
                        createDBInstanceRequest.withVpcSecurityGroupIds(VPCList);
            }
            String subnetGroupName;
            if(!StringUtils.isEmpty(createDBInstanceRequest.getDBSubnetGroupName())) {
                subnetGroupName = createDBInstanceRequest.getDBSubnetGroupName();
            }
            else {
                subnetGroupName = instanceTemplate.getSubnetGroup();
            }
            logger.debug(String.format("Creating %s in subnet group %s",
                    createDBInstanceRequest.getDBInstanceIdentifier(),
                    subnetGroupName));
            dbInstance = rdsClient.createDBInstance(createDBInstanceRequest);
            logger.debug(String.format("Created %s", dbInstance.getDBInstanceIdentifier()));
        } catch (Exception e) {
            logger.error(String.format(
                    "Error in the RDS client instance creation for %s", instanceId));
            throw new InstanceCreationException(e.getMessage());
        }

        return dbInstance;
    }

    public boolean isReady(DBInstance dbInstance) {
        return READY_RDS_STATUS_SET.contains(dbInstance.getDBInstanceStatus());
    }

    public boolean isAvailable(DBInstance dbInstance) {
        return "available".equals(dbInstance.getDBInstanceStatus());
    }

    public Optional<DBInstance> findSharedInstance() {

        final Timer.Context context = findDefaultInstanceTimer.time();
        try {

            double leastCPU =  Double.MAX_VALUE;
            Optional<DBInstance> bestInstanceId = Optional.empty();
            double currentCPU;
            //int currentConnections;
            DescribeDBInstancesRequest request = new DescribeDBInstancesRequest();
            DescribeDBInstancesResult result;

            do{
                result = rdsClient.describeDBInstances(request);

                for (DBInstance dbInstance : result.getDBInstances()) {

                    //Need to limit shared instance search to appropriate color of deploy
                    if(!dbInstance.getDBInstanceIdentifier()
                            .contains(dBaaSConfiguration.getSharedInstanceDeployColor())){
                        logger.debug(String.format("%s deemed not an appropriate shared instance for " +
                                "current deploy use", dbInstance.getDBInstanceIdentifier()));
                        continue;
                    }
                    logger.debug(String.format("%s determined to be an appropriate instance for " +
                            "shared databases this deploy", dbInstance.getDBInstanceIdentifier()));

                    List<Tag> tags = getInstanceTags(dbInstance);

                    Optional<String> managerTag = tags.stream().filter(t ->
                            t.getKey().equals(DEFAULT_TAG_KEY)).map(Tag::getValue).findFirst();

                    if (isReady(dbInstance) && managerTag.isPresent() &&
                            managerTag.get().equals(dBaaSConfiguration.getManagedServiceTag())) {
                        currentCPU = requestStat(dbInstance.getDBInstanceIdentifier(),
                                CPU_STATISTICS_NAME).orElse(0d).intValue();
                        if (currentCPU < leastCPU) {
                            bestInstanceId = Optional.of(dbInstance);
                            leastCPU = currentCPU;
                        }
                    }

                }

                request.setMarker(result.getMarker());
            } while(result.getMarker() != null);
            return bestInstanceId;
        } finally {
            context.stop();
        }
    }

    public Optional<DBInstance> getDBInstance(String instanceId) {

        DescribeDBInstancesRequest describeDBInstancesRequest = new DescribeDBInstancesRequest()
                .withDBInstanceIdentifier(instanceId);

        Optional<DBInstance> dbInstanceOptional = Optional.empty();

        try {
            DescribeDBInstancesResult result = rdsClient
                    .describeDBInstances(describeDBInstancesRequest);
            if (result.getDBInstances().size() == 1) {
                dbInstanceOptional = Optional.of(result.getDBInstances().get(0));
            }
        } catch (DBInstanceNotFoundException db) {
            logger.error(String.format("Database instance %s not found ", instanceId), db);

        } catch (AmazonServiceException dbInstanceNotFoundException) {
            logger.warn(String.format("Error looking up db instances %s",
                    instanceId), dbInstanceNotFoundException);
        }

        return dbInstanceOptional;
    }

    public void modifyMasterPassword(DBInstance dbInstance, String password) {
        ModifyDBInstanceRequest modifyDBInstanceRequest =
                new ModifyDBInstanceRequest()
                        .withDBInstanceIdentifier(dbInstance.getDBInstanceIdentifier())
                        .withMasterUserPassword(password)
                        .withApplyImmediately(true);
        try {
            rdsClient.modifyDBInstance(modifyDBInstanceRequest);
        } catch (Exception e) {
            throw new DatabaseDeletionException(String.format("Error modifying password on " +
                    "instance %s for soft delete.  Cause- %s", dbInstance.getDBInstanceIdentifier(),
                    e.getMessage()));
        }
    }

    public void deleteInstance(String instanceId){
        try {
            String awsInstanceIdentifier =
                    dBaaSConfiguration.getInstanceIdentifierPrefix() + "-" + instanceId;
            DeleteDBInstanceRequest deleteRequest = new DeleteDBInstanceRequest(awsInstanceIdentifier)
                    //.withFinalDBSnapshotIdentifier(instanceId + "_final_snapshot");
                    .withSkipFinalSnapshot(true);
            rdsClient.deleteDBInstance(deleteRequest);
        }
        catch (Exception e) {
            throw new DatabaseDeletionException(String.format("Deletion of instance %s has " +
                    "failed with cause: %s", instanceId, e.getMessage()));
        }
    }

    private Optional<Double> requestStat(final String instanceId, final String metric) {
        final long twoHrs = 1000 * 60 * 60 * 2;
        final int oneHour = 60 * 60;
        GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
                .withStartTime(new Date(new Date().getTime() - twoHrs))
                .withNamespace("AWS/RDS")
                .withPeriod(oneHour)
                .withDimensions(new Dimension().withName("DBInstanceIdentifier")
                        .withValue(instanceId))
                .withMetricName(metric)
                .withStatistics("Average", "Maximum")
                .withEndTime(new Date());
        GetMetricStatisticsResult result = amazonCloudWatchClient.getMetricStatistics(request);
        return result.getDatapoints().stream().map(Datapoint::getMaximum).max(Double::compare);
    }

    private List<Tag> getInstanceTags(DBInstance dbInstance) {

        try {
            String arn = generateArn(dbInstance);
            ListTagsForResourceResult tagsList
                    = rdsClient.listTagsForResource(new ListTagsForResourceRequest()
                    .withResourceName(arn));
            return tagsList.getTagList();
        } catch (AmazonServiceException e) {
            if (!e.getErrorCode().equals("AccessDenied")) {
                logger.error(String.format("Error getting tags for instance %s",
                        dbInstance.getDBInstanceIdentifier()), e);
            }
        }
        return ImmutableList.of();
    }

    private ImmutableSet<Tag> getTags(String serviceTag, String componentTag, boolean isDefault) {
        ImmutableSet<Tag> tags = new ImmutableSet.Builder<Tag>()
                .add(new Tag().withKey(SERVICE_TAG_KEY).withValue(serviceTag))
                .add(new Tag().withKey(SERVICE_COMPONENT_TAG_KEY).withValue(componentTag))
                .add(new Tag().withKey(MANAGED_BY_SERVICE_COMPONENT_TAG_KEY)
                        .withValue(dBaaSConfiguration.getManagedServiceTag()))
                .add(new Tag().withKey(MAKO_ENVIRONMENT)
                        .withValue(dBaaSConfiguration.getMakoEnvironment()))
                .build();
        // only add a default tag if one of the shared instances
        return isDefault
                ? new ImmutableSet.Builder<Tag>()
                .<Tag>addAll(tags)
                .<Tag>add(new Tag().withKey(DEFAULT_TAG_KEY).withValue(dBaaSConfiguration
                        .getManagedServiceTag())).build()
                : tags;
    }

    private String generateArn(DBInstance dbInstance){
        return String.format("arn:aws:rds:%s:%s:db:%s", dBaaSConfiguration.getAwsRegion().toString(),
                dBaaSConfiguration.getAwsAccountNumber(), dbInstance.getDBInstanceIdentifier());
    }

    public void correctDefaultTagCheck(DBInstance defaultInstance) {

        Set<Tag> configTags = getTags(dBaaSConfiguration.getJiveServiceTag(),
                dBaaSConfiguration.getServiceComponentTag(), true);

        List<Tag> tagList = getInstanceTags(defaultInstance);
        Set<Tag> instanceTags = Sets.newHashSet(tagList);

        Set<Tag> extras = new HashSet<>(instanceTags);
        extras.removeAll(configTags);
        if(!extras.isEmpty()){
            List<String> keys = new ArrayList<>();
            for(Tag extra:extras){
                keys.add(extra.getKey());
            }
            logger.debug(String.format("Removing tags %s from instance %s", keys.toString(),
                    defaultInstance.getDBInstanceIdentifier()));
            RemoveTagsFromResourceRequest removeRequest = new RemoveTagsFromResourceRequest()
                    .withResourceName(generateArn(defaultInstance))
                    .withTagKeys(keys);
            rdsClient.removeTagsFromResource(removeRequest);
        }

        Set<Tag> toUpdate = new HashSet<>(configTags);
        toUpdate.removeAll(instanceTags);
        if(!toUpdate.isEmpty()){
            logger.debug(String.format("Adding default tags to instance %s",
                    defaultInstance.getDBInstanceIdentifier()));
            AddTagsToResourceRequest request = new AddTagsToResourceRequest()
                    .withResourceName(generateArn(defaultInstance))
                    .withTags(configTags);
            rdsClient.addTagsToResource(request);
        }

    }

    public void checkTemplateChanges(DBInstance defaultInstance, Integer allocatedStorage,
                                     String instanceClass) {
        boolean modificationNecessary = false;
        ModifyDBInstanceRequest request = new ModifyDBInstanceRequest()
                .withDBInstanceIdentifier(defaultInstance.getDBInstanceIdentifier())
                .withApplyImmediately(true);
        if(!defaultInstance.getAllocatedStorage().equals(allocatedStorage)){
            if(allocatedStorage < defaultInstance.getAllocatedStorage()){
                logger.error(String.format("Cannot decrease allocated storage on instance. " +
                                "A config change has requested this for instance %s",
                        defaultInstance.getDBInstanceIdentifier()));
                throw new ConfigUpdateException(String.format("Attempt to decrease allocated " +
                        "storage on %s", defaultInstance.getDBInstanceIdentifier()));
            }
            if(allocatedStorage < defaultInstance.getAllocatedStorage()*1.1){
                logger.error(String.format("Any increase to allocated storage must be by at least" +
                                "10 percent.  This is violated with instance %s",
                        defaultInstance.getDBInstanceIdentifier()));
                throw new ConfigUpdateException(String.format("Did not increase allocated " +
                                "storage enough to pass the 10 percent required limit by amazon " +
                                "on instance %s", defaultInstance.getDBInstanceIdentifier()));
            }
            logger.debug(String.format("Attempting to update storage on %s from %d to %d",
                    defaultInstance.getDBInstanceIdentifier(),
                    defaultInstance.getAllocatedStorage(),
                    allocatedStorage));
            request = request.withAllocatedStorage(allocatedStorage);
            modificationNecessary = true;
        }

        if(!defaultInstance.getDBInstanceClass().equals(instanceClass)){
            logger.debug(String.format("Changing instance class on %s from %s to %s",
                    defaultInstance.getDBInstanceIdentifier(),
                    defaultInstance.getDBInstanceClass(),
                    instanceClass));
            request = request.withDBInstanceClass(instanceClass)
            .withApplyImmediately(true);
            modificationNecessary = true;
        }
        if(modificationNecessary){
            rdsClient.modifyDBInstance(request);
        }
    }

    public Instance getInstanceDetails(String instanceId) {

        String officialId = String.format(IDENTIFIER_FORMAT,
                dBaaSConfiguration.getInstanceIdentifierPrefix(),
                instanceId.replace("_", "-"));
        Optional<DBInstance> requestedInstance = getDBInstance(officialId);
        if(!requestedInstance.isPresent()){
            logger.error(String.format("Instance with id %s does not exist in this environment",
                    instanceId));
            throw new InstanceNotFoundException(String.format("Instance with id %s not found",
                    instanceId));
        }

        DBInstance retrievedInstance = requestedInstance.get();
        Integer totalRam = null;
        Integer totalCores = null;
        List<DBaaSConfiguration.InstanceType> instanceTypeList =
                dBaaSConfiguration.getInstanceTypes();
        for(DBaaSConfiguration.InstanceType instanceType : instanceTypeList) {
            if(retrievedInstance.getDBInstanceClass().substring(3)
                    .equals(instanceType.getInstanceClass())){
                totalRam = instanceType.getTotalRam();
                totalCores = instanceType.getTotalCores();
            }
        }
        if(totalCores == null || totalRam == null){
            logger.error(String.format("Invalid instance class found for instance %s", instanceId));
            throw new InstanceClassNotFoundException(String.format("availableinstances endpoint does" +
                    "not contain the instance class %s", retrievedInstance.getDBInstanceClass()));
        }

        return new Instance(retrievedInstance.getDBInstanceIdentifier(),
                retrievedInstance.getDBInstanceClass().substring(3),
                totalRam + RDS_UNITS,
                totalCores,
                retrievedInstance.getAllocatedStorage() + RDS_UNITS,
                retrievedInstance.getDBName(),
                retrievedInstance.getMasterUsername(),
                retrievedInstance.getEndpoint().getAddress(),
                retrievedInstance.getEndpoint().getPort(),
                retrievedInstance.getAvailabilityZone(),
                retrievedInstance.getDBInstanceStatus());
    }
}
