package com.jivesoftware.data.impl;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.Endpoint;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.*;
import com.jivesoftware.data.impl.deletion.InstanceDeleteRequestProcessor;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import com.jivesoftware.data.resources.entities.DatabaseStatus;
import com.jivesoftware.data.resources.entities.UserCreateResponse;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;
import java.util.OptionalInt;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DatabaseManagerTest {

    private DatabaseManager databaseManager;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Mock
    private DatabaseIDHelper databaseIDHelper;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private DatabaseDAO databaseDAO;

    @Mock
    private GetQueueUrlResult getQueueUrlResult;

    @Mock
    private AmazonCloudWatchClient amazonCloudWatchClient;

    @Mock
    private DBInstance dbInstance;

    @Mock
    private Endpoint endpoint;

    @Mock
    private PasswordManager passwordManager;

    @Mock
    private PasswordManager.Instance passwordManagerInstance;

    @Mock
    private InstanceManager instanceManager;

    @Mock
    private DatabaseCreationRequest databaseCreationRequest;

    @Mock
    private DatabaseStatus databaseStatus;

    @Mock
    private Database localDatabase;

    @Mock
    private MasterDatabase masterDatabase;

    @Mock
    private Environment environment;

    @Mock
    private LifecycleEnvironment lifecycleEnvironment;

    @Mock
    private ManagedPeriodicTask managedPeriodicTask;

    @Mock
    private DefaultInstanceLoader.DefaultInstance defaultInstance;

    @Mock
    private DefaultInstanceLoader defaultInstanceDeserializer;

    @Mock
    private DatabaseCreateRequestProcessor databaseCreateRequestProcessor;

    @Mock
    private DatabaseInfo databaseInfo;

    @Mock
    private Database database;

    @Mock
    private PasswordManager.Instance instance;

    @Mock
    private PermissionManager permissionManager;

    @Mock
    private InstanceDeleteRequestProcessor instanceDeleteRequestProcessor;

    @Before
    public void setUp() {
        when(defaultInstanceDeserializer.getInstanceList())
                .thenReturn(ImmutableList.of(defaultInstance));
        when(dBaaSConfiguration.getSharedInstanceDeployColor())
                .thenReturn("red");
        when(defaultInstance.getIdentifier()).thenReturn("testInstance");
        when(databaseIDHelper.getDBInstanceId("red-testInstance"))
                .thenReturn("data-dbaas2-local-dev-red-testInstance");
        when(databaseIDHelper.getDBInstanceId("testInstance"))
                .thenReturn("data-dbaas2-local-dev-testInstance");
        when(defaultInstance.getDbInstanceClass()).thenReturn("testInstanceClass");
        when(passwordManager.getInstance("instanceId"))
                .thenReturn(Optional.of(passwordManagerInstance));
        when(passwordManagerInstance.getUsername()).thenReturn("testUser");
        when(passwordManagerInstance.getPassword()).thenReturn("testPassword");
        when(defaultInstance.getDbName()).thenReturn("testName");
        when(defaultInstance.getAllocatedStorage()).thenReturn(new Integer(10));

        when(instanceManager.createSharedInstance("data-dbaas2-local-dev-red-testInstance",
                "testUser", "testPassword", "postgres")).thenReturn(dbInstance);

        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);

        when(databaseInfo.getDatabase()).thenReturn(database);
        when(databaseInfo.getInstanceId()).thenReturn("testInstance");
        when(database.getSchema()).thenReturn("testSchema");
        when(database.getUser()).thenReturn("testUser");
        when(dbInstance.getEndpoint()).thenReturn(endpoint);
        when(endpoint.getAddress()).thenReturn("testAddress");
        when(endpoint.getPort()).thenReturn(5432);
        when(dbInstance.getMasterUsername()).thenReturn("masterUser");
        when(dbInstance.getDBName()).thenReturn("testName");
        when(databaseIDHelper.getDatabaseInstanceId(dbInstance)).thenReturn("instanceId");
    }

    private void additonalSetUp() {
        when(instanceManager.getDBInstance("data-dbaas2-local-dev-red-testInstance"))
                .thenReturn(Optional.of(dbInstance));
        when(instanceManager.getDBInstance("data-dbaas2-local-dev-testInstance"))
                .thenReturn(Optional.of(dbInstance));
        databaseManager = new DatabaseManager(dBaaSConfiguration, databaseIDHelper, schemaManager,
                databaseDAO, passwordManager, instanceManager, databaseCreateRequestProcessor,
                permissionManager, instanceDeleteRequestProcessor, defaultInstanceDeserializer);
    }

    @Test
    public void noDefaultInstancePresentTest(){
        when(instanceManager.getDBInstance("data-dbaas2-local-dev-red-testInstance"))
                .thenReturn(Optional.empty());
        when(passwordManager.getInstance("red-testInstance"))
                .thenReturn(Optional.of(passwordManagerInstance));
        databaseManager = new DatabaseManager(dBaaSConfiguration, databaseIDHelper, schemaManager,
                databaseDAO, passwordManager, instanceManager, databaseCreateRequestProcessor,
                permissionManager, instanceDeleteRequestProcessor, defaultInstanceDeserializer);
        verify(instanceManager).createSharedInstance("data-dbaas2-local-dev-red-testInstance",
                "testUser", "testPassword", "testName");
    }

    @Test(expected = InstanceCreationException.class)
    public void noDefaultInstancePasswordNotFoundTest() {
        when(instanceManager.getDBInstance("data-dbaas2-local-dev-red-testInstance"))
                .thenReturn(Optional.empty());
        when(passwordManager.getInstance("red-testInstance")).thenReturn(Optional.empty());
        databaseManager = new DatabaseManager(dBaaSConfiguration, databaseIDHelper, schemaManager,
                databaseDAO, passwordManager, instanceManager, databaseCreateRequestProcessor,
                permissionManager, instanceDeleteRequestProcessor, defaultInstanceDeserializer);
    }

    @Test(expected = InstanceCreationException.class)
    public void noDefaultInstanceCreationExceptionTest() {
        when(instanceManager.getDBInstance("data-dbaas2-local-dev-red-testInstance"))
                .thenReturn(Optional.empty());
        when(passwordManager.getInstance("red-testInstance"))
                .thenReturn(Optional.of(passwordManagerInstance));
        when(instanceManager.createSharedInstance(any(), any(), any(), any()))
                .thenThrow(InstanceCreationException.class);
        databaseManager = new DatabaseManager(dBaaSConfiguration, databaseIDHelper, schemaManager,
                databaseDAO, passwordManager, instanceManager, databaseCreateRequestProcessor,
                permissionManager, instanceDeleteRequestProcessor, defaultInstanceDeserializer);
    }

    @Test
    public void defaultInstanceTagUpdateExceptionTest() {
        when(instanceManager.getDBInstance("data-dbaas2-local-dev-red-testInstance"))
                .thenReturn(Optional.of(dbInstance));
        doThrow(Exception.class).when(instanceManager).correctDefaultTagCheck(dbInstance);

        databaseManager = new DatabaseManager(dBaaSConfiguration, databaseIDHelper, schemaManager,
                databaseDAO, passwordManager, instanceManager, databaseCreateRequestProcessor,
                permissionManager, instanceDeleteRequestProcessor, defaultInstanceDeserializer);
    }

    @Test
    public void defaultInstanceTemplateUpdateExceptionTest() {
        when(instanceManager.getDBInstance("data-dbaas2-local-dev-red-testInstance"))
                .thenReturn(Optional.of(dbInstance));
        doThrow(Exception.class).when(instanceManager)
                .checkTemplateChanges(any(), anyInt(), any());

        databaseManager = new DatabaseManager(dBaaSConfiguration, databaseIDHelper, schemaManager,
                databaseDAO, passwordManager, instanceManager, databaseCreateRequestProcessor,
                permissionManager, instanceDeleteRequestProcessor, defaultInstanceDeserializer);
    }

    @Test
    public void createDatabaseTest() {
        additonalSetUp();
        when(databaseCreationRequest.getSourceDatabaseIdOptional())
                .thenReturn(Optional.of("sourceDBId"));
        when(databaseDAO.getDatabaseInfo("sourceDBId")).thenReturn(Optional.of(databaseInfo));
        databaseManager.createDatabase(databaseCreationRequest);

        verify(databaseCreateRequestProcessor).requestDatabaseCreation(databaseCreationRequest);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void createDatabaseExceptionTest() {
        additonalSetUp();
        when(databaseCreationRequest.getSourceDatabaseIdOptional())
                .thenReturn(Optional.of("sourceDBId"));
        when(databaseDAO.getDatabaseInfo("sourceDBId")).thenReturn(Optional.empty());
        databaseManager.createDatabase(databaseCreationRequest);
    }

    @Test
    public void createInstanceTest() {
        additonalSetUp();
        when(databaseCreationRequest.getSourceDatabaseIdOptional())
                .thenReturn(Optional.empty());
        when(databaseCreationRequest.getTenancyType())
                .thenReturn(DatabaseCreationRequest.TenancyType.DEDICATED);
        when(databaseCreationRequest.getInstanceClassOptional())
                .thenReturn(Optional.of("m4.large"));
        when(databaseCreationRequest.getInstanceStorageOptional())
                .thenReturn(Optional.of(100));
        databaseManager.createDatabase(databaseCreationRequest);

        verify(databaseCreateRequestProcessor).requestDatabaseCreation(databaseCreationRequest);
    }

    @Test
    public void createInstanceDefaultsTest() {
        additonalSetUp();
        when(databaseCreationRequest.getSourceDatabaseIdOptional())
                .thenReturn(Optional.empty());
        when(databaseCreationRequest.getTenancyType())
                .thenReturn(DatabaseCreationRequest.TenancyType.DEDICATED);
        when(databaseCreationRequest.getInstanceStorageOptional())
                .thenReturn(Optional.empty());
        when(databaseCreationRequest.getInstanceClassOptional())
                .thenReturn(Optional.empty());
        databaseManager.createDatabase(databaseCreationRequest);

        verify(databaseCreateRequestProcessor).requestDatabaseCreation(databaseCreationRequest);
    }

    @Test
    public void getDatabaseStatusTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.of(databaseStatus));
        assertEquals(databaseManager.getDatabaseStatus("databaseId"), databaseStatus);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void getDatabaseStatusExceptionTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.empty());
        databaseManager.getDatabaseStatus("databaseId");
    }

    @Test
    public void getDatabaseTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.of(databaseStatus));
        when(databaseStatus.getStatus()).thenReturn(DatabaseStatus.Status.READY);
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        assertEquals(databaseManager.getDatabase("databaseId"), database);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void getDatabaseNotPresentTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.empty());
        databaseManager.getDatabase("databaseId");
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void getDatabaseNotReadyTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.of(databaseStatus));
        when(databaseStatus.getStatus()).thenReturn(DatabaseStatus.Status.CREATING);
        databaseManager.getDatabase("databaseId");
    }

    @Test
    public void softDeleteSharedDatabaseTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.SHARED);
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.of(databaseStatus));

        ArgumentCaptor<MasterDatabase> masterDatabaseCaptor =
                ArgumentCaptor.forClass(MasterDatabase.class);

        ArgumentCaptor<DatabaseStatus> databaseStatusCaptor =
                ArgumentCaptor.forClass(DatabaseStatus.class);

        databaseManager.deleteDatabase("databaseId");

        verify(databaseDAO).updateStatus(databaseStatusCaptor.capture());
        assertEquals(databaseStatusCaptor.getValue().getStatus(), DatabaseStatus.Status.DELETED);
        assertEquals(databaseStatusCaptor.getValue().getDatabaseId(), "databaseId");
        verify(schemaManager).changeSchemaPassword(masterDatabaseCaptor.capture(), any(), any(),
                any());
        assertEquals(masterDatabaseCaptor.getValue().getHost(), "testAddress");
        assertEquals(masterDatabaseCaptor.getValue().getUsername(), "masterUser");
        assertEquals(masterDatabaseCaptor.getValue().getPassword(), "testPassword");
        assertEquals(masterDatabaseCaptor.getValue().getPort(), new Integer(5432));
        assertEquals(masterDatabaseCaptor.getValue().getSchema(), "testName");
    }

    @Test(expected = DatabaseDeletionException.class)
    public void softDeleteSharedExceptionTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.SHARED);
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.of(databaseStatus));
        doThrow(SchemaOperationException.class).when(schemaManager)
                .changeSchemaPassword(any(), anyString(), anyString(), anyString());

        databaseManager.deleteDatabase("databaseId");
    }

    @Test
    public void softDeleteDedicatedDatabaseTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getTenancyType())
                .thenReturn(DatabaseCreationRequest.TenancyType.DEDICATED);
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.of(databaseStatus));

        databaseManager.deleteDatabase("databaseId");
        verify(instanceDeleteRequestProcessor).requestSoftDelete("databaseId");
    }

    @Test(expected = DatabaseDeletionException.class)
    public void softDeleteDedicatedDatabaseQueueExceptionTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.of(databaseStatus));
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getTenancyType())
                .thenReturn(DatabaseCreationRequest.TenancyType.DEDICATED);

        doThrow(QueueSendingException.class).when(instanceDeleteRequestProcessor)
                .requestSoftDelete("databaseId");
        databaseManager.deleteDatabase("databaseId");
    }

    @Test(expected = Exception.class)
    public void softDeleteDedicatedDatabaseStatusExceptionTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getTenancyType())
                .thenReturn(DatabaseCreationRequest.TenancyType.DEDICATED);

        doThrow(Exception.class).when(instanceDeleteRequestProcessor)
                .requestSoftDelete("databaseId");
        databaseManager.deleteDatabase("databaseId");
    }

    @Test
    public void softDeleteAlreadyDeletedTest() {
        additonalSetUp();
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.of(databaseStatus));
        when(databaseStatus.getStatus()).thenReturn(DatabaseStatus.Status.DELETED);

        databaseManager.deleteDatabase("databaseId");

        verify(databaseDAO, times(0)).updateStatus(any());
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void softDeleteNoRecordExists() {
        additonalSetUp();
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.empty());
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.of(databaseStatus));

        databaseManager.deleteDatabase("databaseId");
    }

    @Test
    public void softDeleteInstanceMissing() {
        additonalSetUp();
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseDAO.getDatabaseStatus("databaseId")).thenReturn(Optional.of(databaseStatus));
        when(instanceManager.getDBInstance("data-dbaas2-local-dev-testInstance"))
                .thenReturn(Optional.empty());

        databaseManager.deleteDatabase("databaseId");

        ArgumentCaptor<DatabaseStatus> databaseStatusCaptor =
                ArgumentCaptor.forClass(DatabaseStatus.class);

        verify(databaseDAO).updateStatus(databaseStatusCaptor.capture());
        assertEquals(databaseStatusCaptor.getValue().getStatus(), DatabaseStatus.Status.DELETED);
    }

    @Test(expected = TokenNotAuthorizedException.class)
    public void hardDeleteNotAuthTest() {
        additonalSetUp();
        when(permissionManager.isAllowed(any())).thenReturn(false);
        databaseManager.hardDeleteDatabase("anything");
    }

    @Test
    public void hardDeleteSchemaExceptionTest() {
        additonalSetUp();
        DatabaseManager databaseManagerSpy = spy(databaseManager);
        when(permissionManager.isAllowed(any())).thenReturn(true);
        when(databaseDAO.getDeletedDatabases()).thenReturn(ImmutableList.of(database));
        when(database.getId()).thenReturn("databaseId");
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getInstanceId()).thenReturn("instanceId");
        when(databaseIDHelper.getDBInstanceId("instanceId")).thenReturn("dbInstanceId");
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(databaseInfo.getDatabase()).thenReturn(database);
        when(database.getSchema()).thenReturn("schema");
        when(databaseInfo.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.SHARED);
        doReturn(masterDatabase).when(databaseManagerSpy).getMasterDatabase(dbInstance,
                DatabaseCreationRequest.TenancyType.SHARED);
        when(schemaManager.isSchemaExists(masterDatabase, "schema")).thenReturn(true);
        when(database.getUser()).thenReturn("user");
        doThrow(SchemaOperationException.class).when(schemaManager).hardDeleteSchema(masterDatabase,
                "user", "schema");

        databaseManagerSpy.hardDeleteDatabase("token");

        verify(databaseDAO, times(0)).deleteDatabaseRecord(any());

    }

    @Test
    public void hardDeleteOtherExceptionTest() {
        additonalSetUp();
        DatabaseManager databaseManagerSpy = spy(databaseManager);
        when(permissionManager.isAllowed(any())).thenReturn(true);
        when(databaseDAO.getDeletedDatabases()).thenReturn(ImmutableList.of(database));
        when(database.getId()).thenReturn("databaseId");
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getInstanceId()).thenReturn("instanceId");
        when(databaseIDHelper.getDBInstanceId("instanceId")).thenReturn("dbInstanceId");
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(databaseInfo.getDatabase()).thenReturn(database);
        when(database.getSchema()).thenReturn("schema");
        when(databaseInfo.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.SHARED);
        doReturn(masterDatabase).when(databaseManagerSpy).getMasterDatabase(dbInstance,
                DatabaseCreationRequest.TenancyType.SHARED);
        when(schemaManager.isSchemaExists(masterDatabase, "schema")).thenReturn(true);
        when(database.getUser()).thenReturn("user");
        doThrow(Exception.class).when(databaseDAO).deleteDatabaseRecord("databaseId");

        databaseManagerSpy.hardDeleteDatabase("token");
    }

    @Test
    public void hardDeleteSchemaDoesntExistTest() {
        additonalSetUp();
        DatabaseManager databaseManagerSpy = spy(databaseManager);
        when(permissionManager.isAllowed(any())).thenReturn(true);
        when(databaseDAO.getDeletedDatabases()).thenReturn(ImmutableList.of(database));
        when(database.getId()).thenReturn("databaseId");
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getInstanceId()).thenReturn("instanceId");
        when(databaseIDHelper.getDBInstanceId("instanceId")).thenReturn("dbInstanceId");
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(databaseInfo.getDatabase()).thenReturn(database);
        when(database.getSchema()).thenReturn("schema");
        when(databaseInfo.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.SHARED);
        doReturn(masterDatabase).when(databaseManagerSpy).getMasterDatabase(dbInstance,
                DatabaseCreationRequest.TenancyType.SHARED);
        when(schemaManager.isSchemaExists(masterDatabase, "schema")).thenReturn(false);
        databaseManagerSpy.hardDeleteDatabase("token");
        verify(databaseDAO).deleteDatabaseRecord("databaseId");

    }

    @Test
    public void hardDeleteInstanceNotFoundTest() {
        additonalSetUp();
        when(permissionManager.isAllowed(any())).thenReturn(true);
        when(databaseDAO.getDeletedDatabases()).thenReturn(ImmutableList.of(database));
        when(database.getId()).thenReturn("databaseId");
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getInstanceId()).thenReturn("instanceId");
        when(databaseIDHelper.getDBInstanceId("instanceId")).thenReturn("dbInstanceId");
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.empty());
        databaseManager.hardDeleteDatabase("token");
        verify(databaseDAO).deleteDatabaseRecord("databaseId");

    }

    @Test
    public void hardDeleteInstanceDeletionExceptionTest() {
        additonalSetUp();
        DatabaseManager databaseManagerSpy = spy(databaseManager);
        when(permissionManager.isAllowed(any())).thenReturn(true);
        when(databaseDAO.getDeletedDatabases()).thenReturn(ImmutableList.of(database));
        when(database.getId()).thenReturn("databaseId");
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getInstanceId()).thenReturn("instanceId");
        when(databaseIDHelper.getDBInstanceId("instanceId")).thenReturn("dbInstanceId");
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(databaseInfo.getDatabase()).thenReturn(database);
        when(database.getSchema()).thenReturn("schema");
        when(databaseInfo.getTenancyType()).thenReturn(
                DatabaseCreationRequest.TenancyType.DEDICATED);
        doReturn(masterDatabase).when(databaseManagerSpy).getMasterDatabase(dbInstance,
                DatabaseCreationRequest.TenancyType.DEDICATED);
        when(schemaManager.isSchemaExists(masterDatabase, "schema")).thenReturn(false);
        doThrow(DatabaseDeletionException.class).when(instanceManager).deleteInstance(anyString());

        databaseManager.hardDeleteDatabase("token");
        verify(databaseDAO, times(0)).deleteDatabaseRecord(any());
    }

    @Test
    public void hardDeleteDedicatedAllSuccessTest() {
        additonalSetUp();
        DatabaseManager databaseManagerSpy = spy(databaseManager);
        when(permissionManager.isAllowed(any())).thenReturn(true);
        when(databaseDAO.getDeletedDatabases()).thenReturn(ImmutableList.of(database));
        when(database.getId()).thenReturn("databaseId");
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getInstanceId()).thenReturn("instanceId");
        when(databaseIDHelper.getDBInstanceId("instanceId")).thenReturn("dbInstanceId");
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(databaseInfo.getDatabase()).thenReturn(database);
        when(database.getSchema()).thenReturn("schema");
        when(databaseInfo.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.DEDICATED);
        doReturn(masterDatabase).when(databaseManagerSpy).getMasterDatabase(dbInstance,
                DatabaseCreationRequest.TenancyType.DEDICATED);
        when(schemaManager.isSchemaExists(masterDatabase, "schema")).thenReturn(true);
        when(database.getUser()).thenReturn("user");

        databaseManagerSpy.hardDeleteDatabase("token");

        verify(instanceManager).deleteInstance("instanceId");
        verify(databaseDAO).deleteDatabaseRecord("databaseId");

    }

    @Test
    public void hardDeleteSharedAllSuccessTest() {
        additonalSetUp();
        DatabaseManager databaseManagerSpy = spy(databaseManager);
        when(permissionManager.isAllowed(any())).thenReturn(true);
        when(databaseDAO.getDeletedDatabases()).thenReturn(ImmutableList.of(database));
        when(database.getId()).thenReturn("databaseId");
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getInstanceId()).thenReturn("instanceId");
        when(databaseIDHelper.getDBInstanceId("instanceId")).thenReturn("dbInstanceId");
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(databaseInfo.getDatabase()).thenReturn(database);
        when(database.getSchema()).thenReturn("schema");
        when(databaseInfo.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.SHARED);
        doReturn(masterDatabase).when(databaseManagerSpy).getMasterDatabase(dbInstance,
                DatabaseCreationRequest.TenancyType.SHARED);
        when(schemaManager.isSchemaExists(masterDatabase, "schema")).thenReturn(true);
        when(database.getUser()).thenReturn("user");

        databaseManagerSpy.hardDeleteDatabase("token");
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void createUserDatabaseRecordMissingTest() {
        additonalSetUp();
        when(passwordManager.generatePassword()).thenReturn("password");
        when(databaseDAO.getDatabaseInfo("username")).thenReturn(Optional.empty());
        when(databaseDAO.getDatabaseStatus(any())).thenReturn(Optional.of(databaseStatus));

        databaseManager.createUser("username");
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void createUserInvalidStatusTest() {
        additonalSetUp();
        when(passwordManager.generatePassword()).thenReturn("password");
        when(databaseDAO.getDatabaseInfo("username")).thenReturn(Optional.of(databaseInfo));
        when(databaseDAO.getDatabaseStatus(any())).thenReturn(Optional.of(databaseStatus));
        when(databaseStatus.getStatus()).thenReturn(DatabaseStatus.Status.CREATING);

        databaseManager.createUser("username");
    }

    @Test(expected = SchemaOperationException.class)
    public void createUserSchemaExceptionTest() {
        additonalSetUp();
        when(passwordManager.generatePassword()).thenReturn("password");
        when(databaseDAO.getDatabaseInfo("username")).thenReturn(Optional.of(databaseInfo));
        when(databaseDAO.getDatabaseStatus(any())).thenReturn(Optional.of(databaseStatus));
        when(databaseStatus.getStatus()).thenReturn(DatabaseStatus.Status.READY);
        when(databaseIDHelper.getDBInstanceId("instanceId")).thenReturn("dbInstanceId");

        doThrow(SchemaOperationException.class).when(schemaManager).createUser(any(), any(), any(),
                any());

        databaseManager.createUser("username");

    }

    @Test
    public void createUserTest() {
        additonalSetUp();
        when(passwordManager.generatePassword()).thenReturn("password");
        when(databaseDAO.getDatabaseInfo("username")).thenReturn(Optional.of(databaseInfo));
        when(databaseDAO.getDatabaseStatus(any())).thenReturn(Optional.of(databaseStatus));
        when(databaseStatus.getStatus()).thenReturn(DatabaseStatus.Status.READY);
        when(databaseIDHelper.getDBInstanceId("instanceId")).thenReturn("dbInstanceId");

        UserCreateResponse controlUserCreateResponse = new UserCreateResponse("username", "password");

        UserCreateResponse userCreateResponse = databaseManager.createUser("username");

        assertEquals(controlUserCreateResponse.getPassword(), userCreateResponse.getPassword());
        verify(schemaManager).createUser(any(), any(), any(), any());
    }

}






















