package com.jivesoftware.data.impl.deletion;

import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.Endpoint;
import com.jivesoftware.data.exceptions.SchemaOperationException;
import com.jivesoftware.data.impl.DatabaseIDHelper;
import com.jivesoftware.data.impl.InstanceManager;
import com.jivesoftware.data.impl.SchemaManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class DeleteCompleteProcessorTest {

    private DeleteCompleteProcessor deleteCompleteProcessor;

    @Mock
    private InstanceManager instanceManager;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private DatabaseIDHelper databaseIDHelper;

    @Mock
    private DBInstance dbInstance;

    @Mock
    private Endpoint endpoint;


    @Before
    public void setUp() {
        when(databaseIDHelper.getDBInstanceId("databaseId")).thenReturn("dbInstanceId");
        when(dbInstance.getDBName()).thenReturn("dbName");
        when(dbInstance.getMasterUsername()).thenReturn("masterUser");
        when(dbInstance.getEndpoint()).thenReturn(endpoint);
        when(endpoint.getAddress()).thenReturn("address");
        when(endpoint.getPort()).thenReturn(5432);
        deleteCompleteProcessor = new DeleteCompleteProcessor(
                instanceManager, schemaManager, databaseIDHelper);
    }

    @Test
    public void deleteInstanceNotFoundTest() {
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.empty());
        assertEquals(deleteCompleteProcessor.process("databaseId", "password"),
                Optional.of(DeletionStep.RESETTING_PASSWORD));
    }

    @Test
    public void deleteInstanceNotReadyTest() {
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(instanceManager.isReady(dbInstance)).thenReturn(false);
        assertEquals(deleteCompleteProcessor.process("databaseId", "password"),
                Optional.of(DeletionStep.RESETTING_PASSWORD));
    }

    @Test(expected = SchemaOperationException.class)
    public void deleteInstanceSchemaException() {
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(instanceManager.isReady(dbInstance)).thenReturn(true);
        doThrow(SchemaOperationException.class).when(schemaManager).changeSchemaPassword(
                any(), anyString(), anyString(), anyString());
        deleteCompleteProcessor.process("databaseId", "password");
    }

    @Test
    public void deleteInstanceIsReadyTest() {
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(instanceManager.isReady(dbInstance)).thenReturn(true);
        assertEquals(deleteCompleteProcessor.process("databaseId", "password"), Optional.empty());
    }
}
