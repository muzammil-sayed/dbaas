package com.jivesoftware.data.impl.deletion;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.impl.DatabaseIDHelper;
import com.jivesoftware.data.impl.InstanceManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class DeleteReadyProcessorTest {

    private DeleteReadyProcessor deleteReadyProcessor;

    @Mock
    private InstanceManager instanceManager;

    @Mock
    private DatabaseIDHelper databaseIDHelper;

    @Mock
    private DBInstance dbInstance;

    @Before
    public void setUp() {
        when(databaseIDHelper.getDBInstanceId("databaseId")).thenReturn("dbInstanceId");
        deleteReadyProcessor = new DeleteReadyProcessor(instanceManager, databaseIDHelper);
    }

    @Test
    public void instanceNotFoundTest() {
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.empty());
        assertEquals(deleteReadyProcessor.process("databaseId", "password"),
                Optional.of(DeletionStep.PREPARING));
    }

    @Test
    public void instanceNotAvailableTest() {
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(instanceManager.isAvailable(dbInstance)).thenReturn(false);
        assertEquals(deleteReadyProcessor.process("databaseId", "password"),
                Optional.of(DeletionStep.PREPARING));
    }

    @Test
    public void deleteReadySuccessTest() {
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        when(instanceManager.isAvailable(dbInstance)).thenReturn(true);
        assertEquals(deleteReadyProcessor.process("databaseId", "password"),
                Optional.of(DeletionStep.DELETING));
    }

}
