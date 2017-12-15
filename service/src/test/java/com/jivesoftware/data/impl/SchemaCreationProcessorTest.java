package com.jivesoftware.data.impl;

import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.Endpoint;
import com.jivesoftware.data.exceptions.SchemaOperationException;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;

@RunWith(MockitoJUnitRunner.class)
public class SchemaCreationProcessorTest {

    private SchemaCreationProcessor schemaCreationProcessor;

    @Mock
    private InstanceManager instanceManager;

    @Mock
    private DatabaseDAO databaseDAO;

    @Mock
    private DatabaseIDHelper databaseIDHelper;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private PasswordManager passwordManager;

    @Mock
    private MasterDatabase masterDatabase;

    @Mock
    private DBInstance dbInstance;

    @Mock
    private Endpoint endpoint;

    @Mock
    private PasswordManager.Instance instance;

    @Mock
    private DatabaseCreationRequest databaseCreationRequest;

    @Mock
    private DatabaseInfo databaseInfo;

    @Before
    public void setup(){

        when(databaseIDHelper.getDatabaseInstanceId(dbInstance)).thenReturn("InstanceId");
        when(passwordManager.getInstance("InstanceId")).thenReturn(Optional.of(instance));
        when(instance.getPassword()).thenReturn("InstancePassword");
        when(dbInstance.getEndpoint()).thenReturn(endpoint);
        when(endpoint.getAddress()).thenReturn("InstanceAddress");
        when(dbInstance.getMasterUsername()).thenReturn("MasterUser");
        when(endpoint.getPort()).thenReturn(5432);
        when(dbInstance.getDBName()).thenReturn("DBName");
        when(databaseCreationRequest.getDataLocality()).thenReturn(
                DatabaseCreationRequest.DataLocality.US);
        when(databaseCreationRequest.getServiceTag()).thenReturn("ServiceTag");


        schemaCreationProcessor = new SchemaCreationProcessor(instanceManager, databaseDAO,
                databaseIDHelper,
                schemaManager,
                passwordManager);
    }

    @Test(expected = SchemaOperationException.class)
    public void noSharedInstanceTest() throws Exception {
        when(databaseCreationRequest.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.SHARED);
        when(instanceManager.findSharedInstance()).thenReturn(Optional.empty());

        schemaCreationProcessor.process("databaseId", "password", databaseCreationRequest);
        verify(instanceManager, times(0)).getDBInstance(any());
    }

    @Test
    public void sharedInstanceCloneTest() throws Exception {
        when(databaseCreationRequest.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.SHARED);
        when(instanceManager.findSharedInstance()).thenReturn(Optional.of(dbInstance));
        when(databaseCreationRequest.getSourceDatabaseIdOptional()).thenReturn(Optional.of("SourceDBIDExistsforCloning"));

        assertEquals(schemaCreationProcessor.process("databaseId", "password", databaseCreationRequest), Optional.of(CreationStep.CLONE));
        verify(instanceManager, times(0)).getDBInstance(any());
    }

    @Test
    public void sharedInstanceSoloTest() throws Exception {
        when(databaseCreationRequest.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.SHARED);
        when(instanceManager.findSharedInstance()).thenReturn(Optional.of(dbInstance));
        when(databaseCreationRequest.getDataLocality()).thenReturn(
                DatabaseCreationRequest.DataLocality.US);
        when(databaseCreationRequest.getServiceTag()).thenReturn("ServiceTag");
        when(databaseCreationRequest.getSourceDatabaseIdOptional()).thenReturn(Optional.empty());

        assertEquals(schemaCreationProcessor.process("databaseId", "password", databaseCreationRequest), Optional.empty());
        verify(instanceManager, times(0)).getDBInstance(any());
    }

    @Test(expected = SchemaOperationException.class)
    public void dedicatedInstanceNotFoundTest() throws Exception {
        when(databaseCreationRequest.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.DEDICATED);
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.empty());

        schemaCreationProcessor.process("databaseId", "password", databaseCreationRequest);
        verify(instanceManager, times(0)).findSharedInstance();
    }

    @Test
    public void dedicatedInstanceCloneTest() throws Exception {
        when(databaseCreationRequest.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.DEDICATED);
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getInstanceId()).thenReturn("databaseIdentifier");
        when(databaseIDHelper.getDBInstanceId("databaseIdentifier")).thenReturn("databaseId");
        when(instanceManager.getDBInstance("databaseId")).thenReturn(Optional.of(dbInstance));

        when(databaseCreationRequest.getSourceDatabaseIdOptional()).thenReturn(Optional.of("SourceDBIDExistsforCloning"));

        assertEquals(schemaCreationProcessor.process("databaseId", "password", databaseCreationRequest), Optional.of(CreationStep.CLONE));
        verify(instanceManager, times(0)).findSharedInstance();
    }

    @Test
    public void dedicatedInstanceSoloTest() throws Exception {
        when(databaseCreationRequest.getTenancyType()).thenReturn(DatabaseCreationRequest.TenancyType.DEDICATED);
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(databaseInfo.getInstanceId()).thenReturn("databaseIdentifier");
        when(databaseIDHelper.getDBInstanceId("databaseIdentifier")).thenReturn("databaseId");
        when(instanceManager.getDBInstance("databaseId")).thenReturn(Optional.of(dbInstance));

        when(databaseCreationRequest.getSourceDatabaseIdOptional()).thenReturn(Optional.empty());

        assertEquals(schemaCreationProcessor.process("databaseId", "password", databaseCreationRequest), Optional.empty());
        verify(instanceManager, times(0)).findSharedInstance();
    }

}
