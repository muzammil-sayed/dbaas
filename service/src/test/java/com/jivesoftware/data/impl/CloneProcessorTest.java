package com.jivesoftware.data.impl;

import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.Endpoint;
import com.jivesoftware.data.exceptions.CloneException;
import com.jivesoftware.data.exceptions.DatabaseNotFoundException;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CloneProcessorTest {

    private CloneProcessor cloneProcessor;

    @Mock
    private InstanceManager instanceManager;

    @Mock
    private DatabaseDAO databaseDAO;

    @Mock
    private DatabaseIDHelper databaseIDHelper;

    @Mock
    private CloneManager cloneManager;

    @Mock
    private PasswordManager passwordManager;

    @Mock
    private DatabaseInfo targetDatabaseInfo;

    @Mock
    private DatabaseInfo sourceDatabaseInfo;

    @Mock
    private DBInstance targetDBInstance;

    @Mock
    private DBInstance sourceDBInstance;

    @Mock
    private Database sourceDatabase;

    @Mock
    private Database targetDatabase;

    @Mock
    private MasterDatabase targetMasterDatabase;

    @Mock
    private MasterDatabase sourceMasterDatabase;

    @Mock
    private DatabaseCreationRequest databaseCreationRequest;

    @Mock
    private PasswordManager.Instance targetInstance;

    @Mock
    private PasswordManager.Instance sourceInstance;

    @Mock
    private Endpoint targetEndpoint;

    @Mock
    private Endpoint sourceEndpoint;


    @Before
    public void setup(){
        cloneProcessor = new CloneProcessor(instanceManager, databaseDAO,
                databaseIDHelper, cloneManager, passwordManager);

        when(targetDatabaseInfo.getInstanceId()).thenReturn("instanceId");
        when(databaseIDHelper.getDBInstanceId("instanceId")).thenReturn("dbInstanceId");
        when(instanceManager.getDBInstance("dbInstanceId"))
                .thenReturn(Optional.of(targetDBInstance));
        when(databaseIDHelper.getDatabaseInstanceId(targetDBInstance))
                .thenReturn("passwordInstanceId");
        when(passwordManager.getInstance("passwordInstanceId"))
                .thenReturn(Optional.of(targetInstance));
        when(targetInstance.getPassword()).thenReturn("password");
        when(targetDBInstance.getEndpoint()).thenReturn(targetEndpoint);
        when(targetEndpoint.getAddress()).thenReturn("address");
        when(targetDBInstance.getMasterUsername()).thenReturn("masterUsername");
        when(targetEndpoint.getPort()).thenReturn(5432);
        when(targetDBInstance.getDBName()).thenReturn("dbName");

        when(sourceDatabaseInfo.getDatabase()).thenReturn(sourceDatabase);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void sourceDBINotCorrectlyPassedTest() throws Exception {
        when(databaseCreationRequest.getSourceDatabaseIdOptional()).thenReturn(Optional.empty());

        cloneProcessor.process("databaseId", "password", databaseCreationRequest);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void targetDBNotFoundTest() throws Exception {
        when(databaseCreationRequest.getSourceDatabaseIdOptional())
                .thenReturn(Optional.of("sourceDatabaseId"));
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.empty());

        cloneProcessor.process("databaseId", "password", databaseCreationRequest);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void sourceDBNotFoundTest() throws Exception {
        when(databaseCreationRequest.getSourceDatabaseIdOptional())
                .thenReturn(Optional.of("sourceDatabaseId"));
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(targetDatabaseInfo));
        when(databaseDAO.getDatabaseInfo("sourceDatabaseId")).thenReturn(Optional.empty());

        cloneProcessor.process("databaseId", "password", databaseCreationRequest);
    }

    @Test
    public void cloneCreationSameHostTest(){
        when(databaseCreationRequest.getSourceDatabaseIdOptional())
                .thenReturn(Optional.of("sourceDatabaseId"));
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(targetDatabaseInfo));
        when(databaseDAO.getDatabaseInfo("sourceDatabaseId"))
                .thenReturn(Optional.of(sourceDatabaseInfo));

        when(sourceDatabaseInfo.getDatabase()).thenReturn(sourceDatabase);

        when(sourceDatabase.getHost()).thenReturn("address");

        when(sourceDatabase.getSchema()).thenReturn("schema");
        when(targetDatabaseInfo.getDatabase()).thenReturn(targetDatabase);

        ArgumentCaptor<MasterDatabase> sourceMasterDatabaseCaptor =
                ArgumentCaptor.forClass(MasterDatabase.class);

        assertEquals(cloneProcessor.process("databaseId", "password", databaseCreationRequest),
                Optional.empty());
        verify(cloneManager).clone(sourceMasterDatabaseCaptor.capture(), any(), any(), any());
        assertTrue(sourceMasterDatabaseCaptor.getValue().getPort() == 5432);
        assertEquals(sourceMasterDatabaseCaptor.getValue().getHost(), "address");
        assertEquals(sourceMasterDatabaseCaptor.getValue().getPassword(), "password");
        assertEquals(sourceMasterDatabaseCaptor.getValue().getSchema(), "dbName");
        assertEquals(sourceMasterDatabaseCaptor.getValue().getUsername(), "masterUsername");
    }

    @Test
    public void cloneCreationDifferentHostTest(){
        when(databaseCreationRequest.getSourceDatabaseIdOptional())
                .thenReturn(Optional.of("sourceDatabaseId"));
        when(databaseDAO.getDatabaseInfo("databaseId"))
                .thenReturn(Optional.of(targetDatabaseInfo));
        when(databaseDAO.getDatabaseInfo("sourceDatabaseId"))
                .thenReturn(Optional.of(sourceDatabaseInfo));

        when(sourceDatabase.getHost()).thenReturn("notSameHost");
        when(targetMasterDatabase.getHost()).thenReturn("differentHost");
        when(sourceDatabase.getId()).thenReturn("sourceDBID");
        when(instanceManager.getDBInstance("sourceDBID"))
                .thenReturn(Optional.of(sourceDBInstance));

        when(sourceDBInstance.getEndpoint()).thenReturn(sourceEndpoint);
        when(databaseIDHelper.getDatabaseInstanceId(sourceDBInstance))
                .thenReturn("sourceDBInstanceID");
        when(passwordManager.getInstance("sourceDBInstanceID"))
                .thenReturn(Optional.of(sourceInstance));
        when(sourceInstance.getPassword()).thenReturn("sourcePassword");
        when(sourceEndpoint.getAddress()).thenReturn("sourceAddress");
        when(sourceDBInstance.getMasterUsername()).thenReturn("sourceMasterUsername");
        when(sourceEndpoint.getPort()).thenReturn(5432);
        when(sourceDBInstance.getDBName()).thenReturn("sourceDBName");

        when(sourceDatabase.getSchema()).thenReturn("schema");
        when(targetDatabaseInfo.getDatabase()).thenReturn(targetDatabase);

        ArgumentCaptor<MasterDatabase> sourceMasterDatabaseCaptor = ArgumentCaptor.forClass(MasterDatabase.class);

        assertEquals(cloneProcessor.process("databaseId", "password", databaseCreationRequest), Optional.empty());
        verify(cloneManager).clone(sourceMasterDatabaseCaptor.capture(), any(), any(), any());
        assertNotEquals(sourceMasterDatabaseCaptor.getValue().getHost(), "address");
        assertNotEquals(sourceMasterDatabaseCaptor.getValue().getSchema(), "dbName");
    }

    @Test(expected = CloneException.class)
    public void cloningThrewExceptionTest() {
        when(databaseCreationRequest.getSourceDatabaseIdOptional())
                .thenReturn(Optional.of("sourceDatabaseId"));
        when(databaseDAO.getDatabaseInfo("databaseId")).thenReturn(Optional.of(targetDatabaseInfo));
        when(databaseDAO.getDatabaseInfo("sourceDatabaseId"))
                .thenReturn(Optional.of(sourceDatabaseInfo));

        when(sourceDatabase.getId()).thenReturn("sourceDBID");
        when(instanceManager.getDBInstance("sourceDBID"))
                .thenReturn(Optional.of(sourceDBInstance));

        when(sourceDBInstance.getEndpoint()).thenReturn(sourceEndpoint);
        when(databaseIDHelper.getDatabaseInstanceId(sourceDBInstance))
                .thenReturn("sourceDBInstanceID");
        when(passwordManager.getInstance("sourceDBInstanceID"))
                .thenReturn(Optional.of(sourceInstance));
        when(sourceInstance.getPassword()).thenReturn("sourcePassword");
        when(sourceEndpoint.getAddress()).thenReturn("sourceAddress");
        when(sourceDBInstance.getMasterUsername()).thenReturn("sourceMasterUsername");
        when(sourceEndpoint.getPort()).thenReturn(5432);
        when(sourceDBInstance.getDBName()).thenReturn("sourceDBName");

        when(sourceDatabase.getSchema()).thenReturn("schema");
        when(targetDatabaseInfo.getDatabase()).thenReturn(targetDatabase);

        when(sourceDatabase.getHost()).thenReturn("sameHost");
        when(targetMasterDatabase.getHost()).thenReturn("sameHost");

        doThrow(new CloneException("intended exception")).when(cloneManager).clone(any(), any(), any(), any());
        cloneProcessor.process("databaseId", "password", databaseCreationRequest);
    }
}
