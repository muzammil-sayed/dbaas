package com.jivesoftware.data.impl;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.InstanceCreationException;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class InstanceCreationProcessorTest {

    private InstanceCreationProcessor instanceCreationProcessor;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Mock
    private DatabaseIDHelper databaseIDHelper;

    @Mock
    private InstanceManager instanceManager;

    @Mock
    private DatabaseDAO databaseDAO;

    @Mock
    private DatabaseCreationRequest databaseCreationRequest;

    @Mock
    private DBInstance dbInstance;

    @Mock
    private Database database;

    @Mock
    private DBaaSConfiguration.InstanceTemplate instanceTemplate;

    @Before
    public void setup() {
        instanceCreationProcessor = new InstanceCreationProcessor(
                databaseIDHelper, instanceManager, databaseDAO, dBaaSConfiguration);
        when(databaseIDHelper.getDBInstanceId("databaseId")).thenReturn("test_databaseId");
        when(databaseCreationRequest.getServiceTag()).thenReturn("serviceTag");
        when(databaseCreationRequest.getServiceComponentTag()).thenReturn("serviceComponentTag");
        when(databaseIDHelper.getDatabaseInstanceId(dbInstance)).thenReturn("instanceId");
        when(dbInstance.getMasterUsername()).thenReturn("masterUsername");
        when(dbInstance.getDBName()).thenReturn("dbName");
        when(databaseCreationRequest.getDataLocality()).thenReturn(
                DatabaseCreationRequest.DataLocality.US);
        when(databaseCreationRequest.getTenancyType()).thenReturn(
                DatabaseCreationRequest.TenancyType.DEDICATED);
        when(databaseCreationRequest.getServiceTag()).thenReturn("serviceTag");
    }

    @Test
    public void instanceCreationTest() throws Exception {

        when(databaseCreationRequest.getInstanceClassOptional()).thenReturn(Optional.of("m4.large"));
        when(databaseCreationRequest.getInstanceStorageOptional()).thenReturn(Optional.of(100));

        ArgumentCaptor<String> realId = ArgumentCaptor.forClass(String.class);
        when(instanceManager.createDedicatedInstance(any(), any(), any(), any(), any(), any()))
                .thenReturn(dbInstance);

        ArgumentCaptor<Database> databaseCreated = ArgumentCaptor.forClass(Database.class);

        assertEquals(instanceCreationProcessor
                .process("databaseId", "password", databaseCreationRequest),
                Optional.of(CreationStep.INSTANCE_READY));

        verify(instanceManager).createDedicatedInstance(realId.capture(), any(), any(), any(), any(),
                any());
        verify(databaseDAO).putDatabase(any(), any(), databaseCreated.capture(), any(), any(),
                any());
        assertEquals("test-databaseId", realId.getValue());
        assertEquals("databaseId", databaseCreated.getValue().getId());
        assertEquals("masterUsername", databaseCreated.getValue().getUser());
        assertEquals("pending", databaseCreated.getValue().getHost());
        assertTrue(databaseCreated.getValue().getPort() == -1);
        assertEquals("dbName", databaseCreated.getValue().getSchema());
    }

    @Test(expected = InstanceCreationException.class)
    public void instanceCreationExceptionTest() throws Exception {
        when(databaseCreationRequest.getInstanceClassOptional()).thenReturn(Optional.of("m4.large"));
        when(databaseCreationRequest.getInstanceStorageOptional()).thenReturn(Optional.of(100));
        when(instanceManager.createDedicatedInstance(any(), any(), any(), any(), any(), any()))
                .thenThrow(RuntimeException.class);
        instanceCreationProcessor
                .process("databaseId", "password", databaseCreationRequest);
    }

    @Test
    public void instanceCreationNoSizeIndicatedTest() throws Exception {
        when(databaseCreationRequest.getInstanceClassOptional()).thenReturn(Optional.empty());
        when(dBaaSConfiguration.getInstanceTemplate()).thenReturn(instanceTemplate);
        when(instanceTemplate.getDbInstanceClass()).thenReturn("defaultSize");
        when(databaseCreationRequest.getInstanceStorageOptional()).thenReturn(Optional.of(100));

        ArgumentCaptor<String> realId = ArgumentCaptor.forClass(String.class);
        when(instanceManager.createDedicatedInstance(any(), any(), any(), any(), any(), any()))
                .thenReturn(dbInstance);

        ArgumentCaptor<Database> databaseCreated = ArgumentCaptor.forClass(Database.class);

        assertEquals(instanceCreationProcessor
                        .process("databaseId", "password", databaseCreationRequest),
                Optional.of(CreationStep.INSTANCE_READY));

        verify(instanceManager).createDedicatedInstance(realId.capture(), any(), any(), any(), any(), any());
        verify(databaseDAO).putDatabase(any(), any(), databaseCreated.capture(), any(), any(),
                any());
        assertEquals("test-databaseId", realId.getValue());
        assertEquals("databaseId", databaseCreated.getValue().getId());
        assertEquals("masterUsername", databaseCreated.getValue().getUser());
        assertEquals("pending", databaseCreated.getValue().getHost());
        assertTrue(databaseCreated.getValue().getPort() == -1);
        assertEquals("dbName", databaseCreated.getValue().getSchema());
    }
}
