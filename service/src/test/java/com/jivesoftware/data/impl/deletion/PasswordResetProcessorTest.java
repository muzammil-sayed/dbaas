package com.jivesoftware.data.impl.deletion;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.exceptions.DatabaseDeletionException;
import com.jivesoftware.data.impl.DatabaseIDHelper;
import com.jivesoftware.data.impl.InstanceManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PasswordResetProcessorTest {

    private PasswordResetProcessor passwordResetProcessor;

    @Mock
    private InstanceManager instanceManager;

    @Mock
    private DatabaseIDHelper databaseIDHelper;

    @Mock
    private DBInstance dbInstance;


    @Before
    public void setUp() {
        when(databaseIDHelper.getDBInstanceId("databaseId")).thenReturn("dbInstanceId");
        passwordResetProcessor = new PasswordResetProcessor(instanceManager, databaseIDHelper);
    }

    @Test
    public void instanceNotFoundTest() {
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.empty());
        assertEquals(passwordResetProcessor.process("databaseId", "password"),
                Optional.of(DeletionStep.DELETING));
    }

    @Test(expected = DatabaseDeletionException.class)
    public void modificationExceptionTest() {
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        doThrow(DatabaseDeletionException.class)
                .when(instanceManager).modifyMasterPassword(dbInstance, "password");
        passwordResetProcessor.process("databaseId", "password");
    }

    @Test
    public void passwordResetSuccessTest() {
        when(instanceManager.getDBInstance("dbInstanceId")).thenReturn(Optional.of(dbInstance));
        assertEquals(passwordResetProcessor.process("databaseId", "password"),
                Optional.of(DeletionStep.RESETTING_PASSWORD));
        verify(instanceManager).modifyMasterPassword(dbInstance, "password");
    }

}
