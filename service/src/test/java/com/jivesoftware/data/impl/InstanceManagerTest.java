package com.jivesoftware.data.impl;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.*;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.ConfigUpdateException;
import com.jivesoftware.data.exceptions.DatabaseDeletionException;
import com.jivesoftware.data.exceptions.InstanceClassNotFoundException;
import com.jivesoftware.data.exceptions.InstanceNotFoundException;
import com.jivesoftware.data.resources.entities.Instance;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InstanceManagerTest {

    private InstanceManager instanceManager;

    @Mock
    private AmazonCloudWatchClient amazonCloudWatchClient;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Mock
    private AmazonRDSClient rdsClient;

    @Mock
    private MetricRegistry metricRegistry;

    @Mock
    private Timer timer;

    @Mock
    private Timer.Context context;

    @Mock
    private DBaaSConfiguration.InstanceTemplate instanceTemplate;

    @Mock
    private DBInstance dbInstance;

    @Mock
    private DBInstance dbInstanceLessConnections;

    @Mock
    private DescribeDBInstancesResult describeDBInstancesResult;

    @Mock
    private ListTagsForResourceRequest listTagsForResourceRequest;

    @Mock
    private ListTagsForResourceResult listTagsForResourceResult;

    @Mock
    private GetMetricStatisticsResult getMetricStatisticsResult1;

    @Mock
    private GetMetricStatisticsResult getMetricStatisticsResult2;

    @Mock
    private Datapoint datapoint1;

    @Mock
    private Datapoint datapoint2;

    @Mock
    private List dbInstanceList;

    @Mock
    private AddTagsToResourceResult addTagsToResourceResult;

    @Mock
    private RemoveTagsFromResourceResult removeTagsFromResourceResult;

    @Mock
    private Endpoint endpoint;

    @Mock
    private DBaaSConfiguration.InstanceType instanceType;

    private Tag serviceTag;
    private Tag serviceComponentTag;
    private Tag managedByTag;
    private Tag makoEnvironmentTag;
    private Tag defaultTag;
    private List<Tag> actualTags;


    @Before
    public void setUp() {
        when(dBaaSConfiguration.getInstanceTemplate()).thenReturn(instanceTemplate);
        when(metricRegistry.timer(anyString())).thenReturn(timer);
        when(dBaaSConfiguration.getJiveServiceTag()).thenReturn("dbaas-local");
        when(dBaaSConfiguration.getServiceComponentTag()).thenReturn("dbaas-local");
        when(dBaaSConfiguration.getManagedServiceTag()).thenReturn("data-dbaas");
        when(dBaaSConfiguration.getMakoEnvironment()).thenReturn("local-dev");
        when(dBaaSConfiguration.getAwsRegion()).thenReturn(com.amazonaws.regions.Region.getRegion(Regions.US_WEST_2));
        when(dBaaSConfiguration.getAwsAccountNumber()).thenReturn("072535113705");
        instanceManager = new InstanceManager(rdsClient, dBaaSConfiguration, metricRegistry, amazonCloudWatchClient);

        when(instanceTemplate.getAllocatedStorage()).thenReturn(100);
        when(instanceTemplate.getDbInstanceClass()).thenReturn("testClass");
        when(instanceTemplate.getEngine()).thenReturn("V8");
        when(instanceTemplate.getEngineVersion()).thenReturn("v1");
        when(instanceTemplate.isMultiAZ()).thenReturn(true);
        when(instanceTemplate.isPubliclyAccessible()).thenReturn(false);
        when(instanceTemplate.getDbName()).thenReturn("defaultName");
        when(dbInstance.getDBInstanceIdentifier()).thenReturn("test");
        when(dbInstance.getDBInstanceClass()).thenReturn("testClass");
        when(dbInstance.getAllocatedStorage()).thenReturn(120);
        when(instanceTemplate.getPort()).thenReturn(100);
        when(dBaaSConfiguration.getSharedInstanceDeployColor()).thenReturn("red");

        serviceTag = new Tag().withKey("jive_service")
                .withValue(dBaaSConfiguration.getJiveServiceTag());
        serviceComponentTag = new Tag().withKey("service_component")
                .withValue(dBaaSConfiguration.getServiceComponentTag());
        managedByTag = new Tag().withKey("managed_by_service")
                .withValue(dBaaSConfiguration.getManagedServiceTag());
        makoEnvironmentTag = new Tag().withKey("mako-environment")
                .withValue(dBaaSConfiguration.getMakoEnvironment());
        defaultTag = new Tag().withKey("default_by_service")
                .withValue(dBaaSConfiguration.getManagedServiceTag());
        actualTags = new ArrayList<>();
        actualTags.add(serviceTag);
        actualTags.add(serviceComponentTag);
        actualTags.add(managedByTag);
        actualTags.add(makoEnvironmentTag);
        actualTags.add(defaultTag);
    }

    @Test
    public void testCreateSharedInstance() {

        ArgumentCaptor<CreateDBInstanceRequest> createInstanceCaptor = ArgumentCaptor.forClass(CreateDBInstanceRequest.class);
        when(rdsClient.createDBInstance(createInstanceCaptor.capture())).thenReturn(dbInstance);

        DBInstance createdInstance = instanceManager.createSharedInstance(
                "test", "testUser", "testPassword", "testDb");

        CreateDBInstanceRequest createDBInstanceRequest = createInstanceCaptor.getValue();
        assertEquals("testDb", createDBInstanceRequest.getDBName());
        assertNotNull(createDBInstanceRequest);
        assertNotNull(createdInstance);
        assertEquals("test", createdInstance.getDBInstanceIdentifier());

        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("jive_service").withValue("dbaas-local")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("service_component").withValue("dbaas-local")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("managed_by_service").withValue("data-dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("mako-environment").withValue("local-dev")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("default_by_service").withValue("data-dbaas")));

    }

    @Test
    public void testCreateSmallDedicatedInstance() {
        ArgumentCaptor<CreateDBInstanceRequest> createInstanceCaptor = ArgumentCaptor.forClass(CreateDBInstanceRequest.class);
        when(rdsClient.createDBInstance(createInstanceCaptor.capture())).thenReturn(dbInstance);

        DBInstance createdInstance = instanceManager.createDedicatedInstance(
                "test", "testPassword", "dedicatedTest", "dbaas", "db.t2.small", 100);


        CreateDBInstanceRequest createDBInstanceRequest = createInstanceCaptor.getValue();
        assertEquals("defaultName", createDBInstanceRequest.getDBName());
        assertNotNull(createDBInstanceRequest);
        assertNotNull(createdInstance);
        assertEquals("test", createdInstance.getDBInstanceIdentifier());
        assertNull(createDBInstanceRequest.getStorageEncrypted());

        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("jive_service").withValue("dedicatedTest")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("service_component").withValue("dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("managed_by_service").withValue("data-dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("mako-environment").withValue("local-dev")));
    }

    @Test
    public void testCreateLargeDedicatedInstance() {
        ArgumentCaptor<CreateDBInstanceRequest> createInstanceCaptor = ArgumentCaptor.forClass(CreateDBInstanceRequest.class);
        when(rdsClient.createDBInstance(createInstanceCaptor.capture())).thenReturn(dbInstance);

        DBInstance createdInstance = instanceManager.createDedicatedInstance(
                "test", "testPassword", "dedicatedTest", "dbaas", "m4.large", 100);


        CreateDBInstanceRequest createDBInstanceRequest = createInstanceCaptor.getValue();
        assertEquals("defaultName", createDBInstanceRequest.getDBName());
        assertNotNull(createDBInstanceRequest);
        assertNotNull(createdInstance);
        assertEquals("test", createdInstance.getDBInstanceIdentifier());
        assertTrue(createDBInstanceRequest.getStorageEncrypted());

        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("jive_service").withValue("dedicatedTest")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("service_component").withValue("dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("managed_by_service").withValue("data-dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("mako-environment").withValue("local-dev")));
    }

    @Test
     public void testCreateDedicatedInstanceSecurityGroup() {
        ArgumentCaptor<CreateDBInstanceRequest> createInstanceCaptor = ArgumentCaptor.forClass(CreateDBInstanceRequest.class);
        when(rdsClient.createDBInstance(createInstanceCaptor.capture())).thenReturn(dbInstance);
        when(instanceTemplate.getSecurityGroup()).thenReturn("aws_security_group_id");
        List<String> VPCList = ImmutableList.of("aws_security_group_id");

        DBInstance createdInstance = instanceManager.createDedicatedInstance(
                "test", "testPassword", "dedicatedTest", "dbaas", "t2.small", 100);

        CreateDBInstanceRequest createDBInstanceRequest = createInstanceCaptor.getValue();
        assertEquals("defaultName", createDBInstanceRequest.getDBName());
        assertNotNull(createDBInstanceRequest);
        assertNotNull(createdInstance);
        assertEquals("test", createdInstance.getDBInstanceIdentifier());
        assertEquals(createDBInstanceRequest.getVpcSecurityGroupIds(), VPCList);

        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("jive_service").withValue("dedicatedTest")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("service_component").withValue("dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("managed_by_service").withValue("data-dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("mako-environment").withValue("local-dev")));
    }

    @Test
    public void testCreateDedicatedInstanceNullSecurityGroup() {
        ArgumentCaptor<CreateDBInstanceRequest> createInstanceCaptor = ArgumentCaptor.forClass(CreateDBInstanceRequest.class);
        when(rdsClient.createDBInstance(createInstanceCaptor.capture())).thenReturn(dbInstance);
        when(instanceTemplate.getSecurityGroup()).thenReturn(null);

        DBInstance createdInstance = instanceManager.createDedicatedInstance(
                "test", "testPassword", "dedicatedTest", "dbaas", "t2.small", 100);

        CreateDBInstanceRequest createDBInstanceRequest = createInstanceCaptor.getValue();
        assertEquals("defaultName", createDBInstanceRequest.getDBName());
        assertNotNull(createDBInstanceRequest);
        assertNotNull(createdInstance);
        assertEquals("test", createdInstance.getDBInstanceIdentifier());
        assertTrue(createDBInstanceRequest.getVpcSecurityGroupIds().isEmpty());

        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("jive_service").withValue("dedicatedTest")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("service_component").withValue("dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("managed_by_service").withValue("data-dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("mako-environment").withValue("local-dev")));
    }

    @Test
    public void testCreateDedicatedInstanceSubnetGroup() {
        ArgumentCaptor<CreateDBInstanceRequest> createInstanceCaptor = ArgumentCaptor.forClass(CreateDBInstanceRequest.class);
        when(rdsClient.createDBInstance(createInstanceCaptor.capture())).thenReturn(dbInstance);
        when(instanceTemplate.getSubnetGroup()).thenReturn("aws_subnet");

        DBInstance createdInstance = instanceManager.createDedicatedInstance(
                "test", "testPassword", "dedicatedTest", "dbaas", "t2.small", 100);

        CreateDBInstanceRequest createDBInstanceRequest = createInstanceCaptor.getValue();
        assertEquals("defaultName", createDBInstanceRequest.getDBName());
        assertNotNull(createDBInstanceRequest);
        assertNotNull(createdInstance);
        assertEquals("test", createdInstance.getDBInstanceIdentifier());
        assertEquals(createDBInstanceRequest.getDBSubnetGroupName(), "aws_subnet");

        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("jive_service").withValue("dedicatedTest")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("service_component").withValue("dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("managed_by_service").withValue("data-dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("mako-environment").withValue("local-dev")));
    }

    @Test
    public void testCreateDedicatedInstanceNullSubnetGroup() {
        ArgumentCaptor<CreateDBInstanceRequest> createInstanceCaptor = ArgumentCaptor.forClass(CreateDBInstanceRequest.class);
        when(rdsClient.createDBInstance(createInstanceCaptor.capture())).thenReturn(dbInstance);
        when(instanceTemplate.getSubnetGroup()).thenReturn(null);

        DBInstance createdInstance = instanceManager.createDedicatedInstance(
                "test", "testPassword", "dedicatedTest", "dbaas", "t2.small", 100);

        CreateDBInstanceRequest createDBInstanceRequest = createInstanceCaptor.getValue();
        assertEquals("defaultName", createDBInstanceRequest.getDBName());
        assertNotNull(createDBInstanceRequest);
        assertNotNull(createdInstance);
        assertEquals("test", createdInstance.getDBInstanceIdentifier());
        assertNull(createDBInstanceRequest.getDBSubnetGroupName());

        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("jive_service").withValue("dedicatedTest")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("service_component").withValue("dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("managed_by_service").withValue("data-dbaas")));
        assertTrue(createDBInstanceRequest.getTags()
                .contains(new Tag().withKey("mako-environment").withValue("local-dev")));
    }

    @Test
    public void testFindLeastConnectionSharedInstance() {

        when(metricRegistry.timer(any()).time()).thenReturn(context);

        when(rdsClient.describeDBInstances()).thenReturn(describeDBInstancesResult);
        when(describeDBInstancesResult.getDBInstances()).thenReturn(
                ImmutableList.of(dbInstance, dbInstanceLessConnections));

        when(dbInstance.getDBInstanceIdentifier()).thenReturn("red-test1");
        when(dbInstanceLessConnections.getDBInstanceIdentifier()).thenReturn("red-test2");

        ArgumentCaptor<ListTagsForResourceRequest> captor = ArgumentCaptor.forClass(ListTagsForResourceRequest.class);
        when(rdsClient.listTagsForResource(captor.capture())).thenReturn(listTagsForResourceResult);
        when(listTagsForResourceResult.getTagList()).thenReturn(ImmutableList.of(defaultTag));


        when(dbInstance.getDBInstanceStatus()).thenReturn("available");
        when(dbInstanceLessConnections.getDBInstanceStatus()).thenReturn("available");

        when(amazonCloudWatchClient.getMetricStatistics(any())).thenAnswer((invocation)->{
            Object[] args = invocation.getArguments();
            for (Object arg : args) {
                //idToUsage.getOrDefault(arg.toString(), 0);
                if(arg.toString().contains("test1")){
                    return getMetricStatisticsResult1;
                }
                if(arg.toString().contains("test2")){
                    return getMetricStatisticsResult2;
                }
            }
            throw new RuntimeException("Seeded values not found in test, what'd you do?");});
        when(getMetricStatisticsResult1.getDatapoints()).thenReturn(ImmutableList.of(datapoint1));
        when(getMetricStatisticsResult2.getDatapoints()).thenReturn(ImmutableList.of(datapoint2));
        when(datapoint1.getMaximum()).thenReturn(10.0);
        when(datapoint2.getMaximum()).thenReturn(8.0);

        when(rdsClient.describeDBInstances(any())).thenReturn(describeDBInstancesResult);
        when(describeDBInstancesResult.getDBInstances()).thenReturn(ImmutableList.of(dbInstance, dbInstanceLessConnections));
        when(context.stop()).thenReturn(1l);

        Optional<DBInstance> testInstance = instanceManager.findSharedInstance();

        assertTrue(testInstance.isPresent());
        assertEquals("red-test2", testInstance.get().getDBInstanceIdentifier());
        assertTrue(captor.getValue().getResourceName().contains("test"));

    }

    @Test
    public void testFindLeastConnectionSharedInstanceMultiColor() {

        when(metricRegistry.timer(any()).time()).thenReturn(context);

        when(rdsClient.describeDBInstances()).thenReturn(describeDBInstancesResult);
        when(describeDBInstancesResult.getDBInstances()).thenReturn(
                ImmutableList.of(dbInstance, dbInstanceLessConnections));

        when(dbInstance.getDBInstanceIdentifier()).thenReturn("red-test1");
        when(dbInstanceLessConnections.getDBInstanceIdentifier()).thenReturn("black-test2");

        ArgumentCaptor<ListTagsForResourceRequest> captor = ArgumentCaptor.forClass(ListTagsForResourceRequest.class);
        when(rdsClient.listTagsForResource(captor.capture())).thenReturn(listTagsForResourceResult);
        when(listTagsForResourceResult.getTagList()).thenReturn(ImmutableList.of(defaultTag));


        when(dbInstance.getDBInstanceStatus()).thenReturn("available");
        when(dbInstanceLessConnections.getDBInstanceStatus()).thenReturn("available");

        when(amazonCloudWatchClient.getMetricStatistics(any())).thenAnswer((invocation)->{
            Object[] args = invocation.getArguments();
            for (Object arg : args) {
                //idToUsage.getOrDefault(arg.toString(), 0);
                if(arg.toString().contains("test1")){
                    return getMetricStatisticsResult1;
                }
                if(arg.toString().contains("test2")){
                    return getMetricStatisticsResult2;
                }
            }
            throw new RuntimeException("Seeded values not found in test, what'd you do?");});
        when(getMetricStatisticsResult1.getDatapoints()).thenReturn(ImmutableList.of(datapoint1));
        when(getMetricStatisticsResult2.getDatapoints()).thenReturn(ImmutableList.of(datapoint2));
        when(datapoint1.getMaximum()).thenReturn(10.0);
        when(datapoint2.getMaximum()).thenReturn(8.0);

        when(rdsClient.describeDBInstances(any())).thenReturn(describeDBInstancesResult);
        when(describeDBInstancesResult.getDBInstances()).thenReturn(ImmutableList.of(dbInstance, dbInstanceLessConnections));
        when(context.stop()).thenReturn(1l);

        Optional<DBInstance> testInstance = instanceManager.findSharedInstance();

        assertTrue(testInstance.isPresent());
        assertEquals("red-test1", testInstance.get().getDBInstanceIdentifier());
        assertTrue(captor.getValue().getResourceName().contains("test"));

    }

    @Test
    public void isAvailableTest() {
        when(dbInstance.getDBInstanceStatus()).thenReturn("available");
        assertTrue(instanceManager.isReady(dbInstance));
        assertTrue(instanceManager.isAvailable(dbInstance));
    }

    @Test
    public void isModifyingTest() {
        when(dbInstance.getDBInstanceStatus()).thenReturn("modifying");
        assertTrue(instanceManager.isReady(dbInstance));
        assertFalse(instanceManager.isAvailable(dbInstance));
    }

    @Test
    public void isBackingUpTest() {
        when(dbInstance.getDBInstanceStatus()).thenReturn("backing-up");
        assertTrue(instanceManager.isReady(dbInstance));
        assertFalse(instanceManager.isAvailable(dbInstance));
    }

    @Test
    public void isMaintenanceTest() {
        when(dbInstance.getDBInstanceStatus()).thenReturn("maintenance");
        assertFalse(instanceManager.isReady(dbInstance));
        assertFalse(instanceManager.isAvailable(dbInstance));
    }

    @Test
    public void isDeletingTest() {
        when(dbInstance.getDBInstanceStatus()).thenReturn("deleting");
        assertFalse(instanceManager.isReady(dbInstance));
        assertFalse(instanceManager.isAvailable(dbInstance));
    }

    @Test
    public void isFailedTest() {
        when(dbInstance.getDBInstanceStatus()).thenReturn("failed");
        assertFalse(instanceManager.isReady(dbInstance));
        assertFalse(instanceManager.isAvailable(dbInstance));
    }

    @Test
    public void getDBInstanceTest() {
        when(rdsClient.describeDBInstances(any())).thenReturn(describeDBInstancesResult);
        when(describeDBInstancesResult.getDBInstances()).thenReturn(dbInstanceList);
        when(dbInstanceList.size()).thenReturn(1);
        when(dbInstanceList.get(0)).thenReturn(dbInstance);

        assertEquals(instanceManager.getDBInstance("instanceId"), Optional.of(dbInstance));
    }

    @Test
    public void getDBInstanceNotFoundTest() throws Exception{

        when(rdsClient.describeDBInstances(any())).
                thenThrow(new DBInstanceNotFoundException("error"));

        instanceManager.getDBInstance("instanceId");

    }

    @Test
    public void getDBInstanceAWSExceptionTest() throws Exception{

        when(rdsClient.describeDBInstances(any())).
                thenThrow(new AmazonServiceException("error"));

        instanceManager.getDBInstance("instanceId");

    }

    @Test
    public void tagsNeedAddingTest() {

        List<Tag> tagList = ImmutableList.of(serviceTag);

        when(rdsClient.listTagsForResource(any())).thenReturn(listTagsForResourceResult);
        when(listTagsForResourceResult.getTagList()).thenReturn(tagList);

        when(rdsClient.removeTagsFromResource(any())).thenReturn(removeTagsFromResourceResult);

        instanceManager.correctDefaultTagCheck(dbInstance);

        ArgumentCaptor<AddTagsToResourceRequest> addRequestCaptor =
                ArgumentCaptor.forClass(AddTagsToResourceRequest.class);

        verify(rdsClient).addTagsToResource(addRequestCaptor.capture());
        verifyZeroInteractions(removeTagsFromResourceResult);
        assertEquals(addRequestCaptor.getValue().getResourceName(),
                "arn:aws:rds:us-west-2:072535113705:db:test");
        assertEquals(addRequestCaptor.getValue().getTags(), actualTags);
    }

    @Test
    public void tagsNeedRemovedTest() {

        Tag wrongTag = new Tag().withKey("bad_tag")
                .withValue("remove_it");

        List<Tag> tagList = actualTags;
        tagList.add(wrongTag);

        when(rdsClient.listTagsForResource(any())).thenReturn(listTagsForResourceResult);
        when(listTagsForResourceResult.getTagList()).thenReturn(tagList);

        when(rdsClient.addTagsToResource(any())).thenReturn(addTagsToResourceResult);

        instanceManager.correctDefaultTagCheck(dbInstance);

        ArgumentCaptor<RemoveTagsFromResourceRequest> removeRequestCaptor =
                ArgumentCaptor.forClass(RemoveTagsFromResourceRequest.class);

        verify(rdsClient).removeTagsFromResource(removeRequestCaptor.capture());
        verifyZeroInteractions(addTagsToResourceResult);
        assertEquals(removeRequestCaptor.getValue().getResourceName(),
                "arn:aws:rds:us-west-2:072535113705:db:test");
        assertEquals(removeRequestCaptor.getValue().getTagKeys().get(0), wrongTag.getKey());
    }

    @Test
    public void tagsNeedAddedAndRemovedTest() {

        Tag wrongTag = new Tag().withKey("bad_tag")
                .withValue("remove_it");

        List<Tag> tagList = new ArrayList<>();
        tagList.add(wrongTag);
        tagList.add(serviceTag);

        when(rdsClient.listTagsForResource(any())).thenReturn(listTagsForResourceResult);
        when(listTagsForResourceResult.getTagList()).thenReturn(tagList);

        instanceManager.correctDefaultTagCheck(dbInstance);

        ArgumentCaptor<AddTagsToResourceRequest> addRequestCaptor =
                ArgumentCaptor.forClass(AddTagsToResourceRequest.class);

        ArgumentCaptor<RemoveTagsFromResourceRequest> removeRequestCaptor =
                ArgumentCaptor.forClass(RemoveTagsFromResourceRequest.class);

        verify(rdsClient).addTagsToResource(addRequestCaptor.capture());
        verify(rdsClient).removeTagsFromResource(removeRequestCaptor.capture());
        assertEquals(removeRequestCaptor.getValue().getResourceName(),
                "arn:aws:rds:us-west-2:072535113705:db:test");
        assertEquals(removeRequestCaptor.getValue().getTagKeys().get(0), wrongTag.getKey());
        assertEquals(addRequestCaptor.getValue().getResourceName(),
                "arn:aws:rds:us-west-2:072535113705:db:test");
        assertEquals(addRequestCaptor.getValue().getTags(), actualTags);
    }

    @Test
    public void tagsFineTest() {

        List<Tag> tagList = actualTags;

        when(rdsClient.listTagsForResource(any())).thenReturn(listTagsForResourceResult);
        when(listTagsForResourceResult.getTagList()).thenReturn(tagList);

        when(rdsClient.addTagsToResource(any())).thenReturn(addTagsToResourceResult);
        when(rdsClient.removeTagsFromResource(any())).thenReturn(removeTagsFromResourceResult);

        instanceManager.correctDefaultTagCheck(dbInstance);

        verifyZeroInteractions(addTagsToResourceResult);
        verifyZeroInteractions(removeTagsFromResourceResult);
    }

    @Test
    public void templateUpdateClassTest() {

        instanceManager.checkTemplateChanges(dbInstance, 120, "instanceClass");

        ArgumentCaptor<ModifyDBInstanceRequest> modifyInstanceRequest =
                ArgumentCaptor.forClass(ModifyDBInstanceRequest.class);

        verify(rdsClient).modifyDBInstance(modifyInstanceRequest.capture());
        assertEquals(modifyInstanceRequest.getValue().getDBInstanceClass(), "instanceClass");
        assertNull(modifyInstanceRequest.getValue().getAllocatedStorage());
        assertTrue(modifyInstanceRequest.getValue().getApplyImmediately());
    }

    @Test
    public void templateUpdateSizeTest() {

        Integer allocatedStorageFromTemplate = 135;

        instanceManager.checkTemplateChanges(dbInstance, allocatedStorageFromTemplate, "testClass");

        ArgumentCaptor<ModifyDBInstanceRequest> modifyInstanceRequest =
                ArgumentCaptor.forClass(ModifyDBInstanceRequest.class);

        verify(rdsClient).modifyDBInstance(modifyInstanceRequest.capture());
        assertEquals(modifyInstanceRequest.getValue().getAllocatedStorage(), allocatedStorageFromTemplate);
        assertNull(modifyInstanceRequest.getValue().getDBInstanceClass());
        assertTrue(modifyInstanceRequest.getValue().getApplyImmediately());
    }

    @Test
    public void templateUpdateBothTest() {

        Integer allocatedStorageFromTemplate = 135;

        instanceManager.checkTemplateChanges(dbInstance, allocatedStorageFromTemplate, "instanceClass");

        ArgumentCaptor<ModifyDBInstanceRequest> modifyInstanceRequest =
                ArgumentCaptor.forClass(ModifyDBInstanceRequest.class);

        verify(rdsClient).modifyDBInstance(modifyInstanceRequest.capture());
        assertEquals(modifyInstanceRequest.getValue().getAllocatedStorage(), allocatedStorageFromTemplate);
        assertEquals(modifyInstanceRequest.getValue().getDBInstanceClass(), "instanceClass");
        assertTrue(modifyInstanceRequest.getValue().getApplyImmediately());
    }

    @Test(expected = ConfigUpdateException.class)
    public void templateUpdateSizeIncreaseExceptionTest() {
        Integer allocatedStorageFromTemplate = 101;

        instanceManager.checkTemplateChanges(dbInstance, allocatedStorageFromTemplate, "testClass");

        ArgumentCaptor<ModifyDBInstanceRequest> modifyInstanceRequest =
                ArgumentCaptor.forClass(ModifyDBInstanceRequest.class);

        verify(rdsClient).modifyDBInstance(modifyInstanceRequest.capture());
        assertEquals(modifyInstanceRequest.getValue().getAllocatedStorage(), allocatedStorageFromTemplate);
        assertNull(modifyInstanceRequest.getValue().getDBInstanceClass());
        assertTrue(modifyInstanceRequest.getValue().getApplyImmediately());

    }

    @Test(expected = DatabaseDeletionException.class)
    public void deleteInstanceExceptionTest() {
        when(rdsClient.deleteDBInstance(any())).thenThrow(RuntimeException.class);
        instanceManager.deleteInstance("instanceId");
    }

    @Test
    public void deleteInstanceSuccessTest() {
        when(dBaaSConfiguration.getInstanceIdentifierPrefix()).thenReturn("prefix");
        ArgumentCaptor<DeleteDBInstanceRequest> deleteRequestCaptor =
                ArgumentCaptor.forClass(DeleteDBInstanceRequest.class);

        instanceManager.deleteInstance("instanceId");
        verify(rdsClient).deleteDBInstance(deleteRequestCaptor.capture());
        assertEquals(deleteRequestCaptor.getValue().getDBInstanceIdentifier(), "prefix-instanceId");
        assertTrue(deleteRequestCaptor.getValue().getSkipFinalSnapshot());
    }

    @Test
    public void modifyMasterPasswordTest() {
        instanceManager.modifyMasterPassword(dbInstance, "password");
        ArgumentCaptor<ModifyDBInstanceRequest> modifyDBInstanceRequestArgumentCaptor =
                ArgumentCaptor.forClass(ModifyDBInstanceRequest.class);
        verify(rdsClient).modifyDBInstance(modifyDBInstanceRequestArgumentCaptor.capture());
        assertTrue(modifyDBInstanceRequestArgumentCaptor.getValue().getApplyImmediately());
        assertEquals(modifyDBInstanceRequestArgumentCaptor.getValue().getDBInstanceIdentifier(),
                "test");
        assertEquals(modifyDBInstanceRequestArgumentCaptor.getValue().getMasterUserPassword(),
                "password");
    }

    @Test(expected = DatabaseDeletionException.class)
    public void modifyMasterPasswordExceptionTest() {
        when(rdsClient.modifyDBInstance(any())).thenThrow(Exception.class);
        instanceManager.modifyMasterPassword(dbInstance, "password");
    }

    @Test(expected = InstanceNotFoundException.class)
    public void getInstanceNotFoundTest() {
        when(dBaaSConfiguration.getInstanceIdentifierPrefix()).thenReturn("prefix");
        when(rdsClient.describeDBInstances(any())).thenReturn(describeDBInstancesResult);
        when(describeDBInstancesResult.getDBInstances()).thenReturn(ImmutableList.of());

        instanceManager.getInstanceDetails("anyID");
    }

    @Test(expected = InstanceClassNotFoundException.class)
    public void getInstanceClassNotValidTest() {
        when(dBaaSConfiguration.getInstanceIdentifierPrefix()).thenReturn("prefix");
        when(rdsClient.describeDBInstances(any())).thenReturn(describeDBInstancesResult);
        when(describeDBInstancesResult.getDBInstances())
                .thenReturn(ImmutableList.of(dbInstance));
        when(dbInstance.getDBInstanceClass()).thenReturn("jibberish");

        instanceManager.getInstanceDetails("anyID");
    }

    @Test
    public void getInstanceSuccessTest() {
        when(dBaaSConfiguration.getInstanceIdentifierPrefix()).thenReturn("prefix");
        when(rdsClient.describeDBInstances(any())).thenReturn(describeDBInstancesResult);
        when(describeDBInstancesResult.getDBInstances())
                .thenReturn(ImmutableList.of(dbInstance));
        when(dBaaSConfiguration.getInstanceTypes())
                .thenReturn(ImmutableList.of(instanceType));
        when(instanceType.getInstanceClass()).thenReturn("m4.large");
        when(instanceType.getTotalCores()).thenReturn(4);
        when(instanceType.getTotalRam()).thenReturn(100);
        when(dbInstance.getDBInstanceClass()).thenReturn("db.m4.large");
        when(dbInstance.getDBInstanceIdentifier()).thenReturn("identifier");
        when(dbInstance.getAllocatedStorage()).thenReturn(100);
        when(dbInstance.getDBName()).thenReturn("dbName");
        when(dbInstance.getMasterUsername()).thenReturn("username");
        when(dbInstance.getEndpoint()).thenReturn(endpoint);
        when(endpoint.getAddress()).thenReturn("host");
        when(endpoint.getPort()).thenReturn(5432);
        when(dbInstance.getAvailabilityZone()).thenReturn("us-west-2");
        when(dbInstance.getDBInstanceStatus()).thenReturn("available");

        Instance retrievedInstance = instanceManager.getInstanceDetails("anyID");

        assertEquals(retrievedInstance.getInstanceIdentifier(), "identifier");
        assertEquals(retrievedInstance.getInstanceClass(), "m4.large");
        assertEquals(retrievedInstance.getTotalRam(), "100GB");
        assertEquals((long)retrievedInstance.getTotalCores(), 4);
        assertEquals(retrievedInstance.getStorage(), "100GB");
        assertEquals(retrievedInstance.getDbName(), "dbName");
        assertEquals(retrievedInstance.getUsername(), "username");
        assertEquals(retrievedInstance.getEndpoint(), "host");
        assertEquals((long)retrievedInstance.getPort(), 5432);
        assertEquals(retrievedInstance.getAvailabilityZone(), "us-west-2");
        assertEquals(retrievedInstance.getInstanceStatus(), "available");
    }



}