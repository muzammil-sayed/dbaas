package com.jivesoftware.data.impl;

import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.Endpoint;
import com.jivesoftware.data.exceptions.DatabaseNotFoundException;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ReadyProcessorTest {

    private ReadyProcessor readyProcessor;

    @Mock
    private InstanceManager instanceManager;

    @Mock
    private DatabaseDAO databaseDAO;

    @Mock
    private DatabaseIDHelper databaseIDHelper;

    @Mock
    private DatabaseCreationRequest databaseCreationRequest;

    @Mock
    private DatabaseInfo databaseInfo;

    @Mock
    private DBInstance dbInstance;

    @Mock
    private Endpoint endpoint;

    @Before
    public void setup(){
        readyProcessor = new ReadyProcessor(instanceManager, databaseDAO, databaseIDHelper);
        when(databaseCreationRequest.getServiceTag()).thenReturn("serviceTag");
        when(databaseInfo.getInstanceId()).thenReturn("instanceID");
        when(databaseCreationRequest.getDataLocality()).thenReturn(
                DatabaseCreationRequest.DataLocality.US);
        when(dbInstance.getMasterUsername()).thenReturn("masterUsername");
        when(dbInstance.getEndpoint()).thenReturn(endpoint);
        when(endpoint.getAddress()).thenReturn("address");
        when(endpoint.getPort()).thenReturn(5432);
        when(dbInstance.getDBName()).thenReturn("dbName");
        when(databaseIDHelper.getDBInstanceId("instanceID")).thenReturn("instanceID");
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void noInstanceFoundInDynamoTest(){
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.empty());

        readyProcessor.process("databaseId", "password", databaseCreationRequest);

    }

    @Test(expected = DatabaseNotFoundException.class)
    public void noInstanceFoundInRDSTest(){
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(instanceManager.getDBInstance("instanceID")).thenReturn(Optional.empty());

        readyProcessor.process("databaseId", "password", databaseCreationRequest);
    }

    @Test
    public void instanceNotReadyTest(){
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(instanceManager.getDBInstance("instanceID")).thenReturn(Optional.of(dbInstance));
        when(instanceManager.isReady(dbInstance)).thenReturn(false);
        assertEquals(readyProcessor.process("databaseId", "password", databaseCreationRequest), Optional.of(CreationStep.INSTANCE_READY));
        verify(databaseDAO, times(0)).putDatabase(any(), any(), any(), any(), any(), any());
    }

    @Test
    public void instanceReadyTest(){
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(databaseInfo));
        when(instanceManager.getDBInstance("instanceID")).thenReturn(Optional.of(dbInstance));
        when(instanceManager.isReady(dbInstance)).thenReturn(true);
        assertEquals(readyProcessor.process("databaseId", "password", databaseCreationRequest), Optional.of(CreationStep.SCHEMA));
    }
}
