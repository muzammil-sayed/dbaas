package com.jivesoftware.data.impl;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.CloneException;
import com.jivesoftware.data.resources.entities.Database;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class CloneManagerTest {

    private CloneManager cloneManager;

    private CloneManager cloneManagerSpy;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Mock
    private MetricRegistry metricRegistry;

    @Mock
    private DBaaSConfiguration.CloneConfiguration cloneConfiguration;

    @Mock
    private com.codahale.metrics.Timer timer;

    @Mock
    private MasterDatabase sourceDatabase;

    @Mock
    private Database targetDatabase;

    @Mock
    private ImmutableMap immutableMap;

    @Mock
    private ImmutableMap.Builder immutableMapBuilder;

    @Mock
    private CommandLine commandLine;

    @Mock
    private org.apache.commons.exec.Executor executor;

    @Mock
    private DefaultExecuteResultHandler resultHandler;

    @Mock
    private com.codahale.metrics.Timer.Context context;

    @Before
    public void setup() {

        Long timeout = 10000L;
        when(dBaaSConfiguration.getCloneConfiguration()).thenReturn(cloneConfiguration);
        when(cloneConfiguration.getExecutionTimeout()).thenReturn(timeout);
        when(metricRegistry.timer(any())).thenReturn(timer);
        when(timer.time()).thenReturn(context);
        when(sourceDatabase.getSchema()).thenReturn("source_schema");
        when(sourceDatabase.getUsername()).thenReturn("source_user");
        when(sourceDatabase.getPassword()).thenReturn("source_password");
        when(sourceDatabase.getHost()).thenReturn("source_host");
        when(sourceDatabase.getPort()).thenReturn(5432);
        when(targetDatabase.getUser()).thenReturn("target_user");
        when(targetDatabase.getHost()).thenReturn("target_host");
        when(targetDatabase.getPort()).thenReturn(5432);
        when(targetDatabase.getSchema()).thenReturn("target_schema");

        //when(immutableMap.builder()).thenReturn(immutableMapBuilder);
        when(immutableMapBuilder.build()).thenReturn(immutableMap);

        when(cloneConfiguration.getCommand())
                .thenReturn("../docker/app/cloneschema.sh ${source_user} " +
                        "${source_password} ${source_host} ${source_port} ${source_schema} " +
                        "${target_user} ${target_password} ${target_host} ${target_port} " +
                        "${target_schema}");
        cloneManager = new CloneManager(dBaaSConfiguration, metricRegistry);
        cloneManagerSpy = spy(cloneManager);
        doReturn(resultHandler).when(cloneManagerSpy).getNewResultHandler();
        doReturn(executor).when(cloneManagerSpy).getNewExecutor();
    }

    @Test
    public void cloneTest() throws Exception {

        String expectedCommand = "[../docker/app/cloneschema.sh, source_user, source_password, " +
                "source_host, 5432, sourceSchema, target_user, targetPassword, target_host, " +
                "5432, target_schema]";

        ArgumentCaptor<CommandLine> commandLineArgumentCaptor =
                ArgumentCaptor.forClass(CommandLine.class);
        ArgumentCaptor<DefaultExecuteResultHandler> defaultExecuteResultHandlerArgumentCaptor =
                ArgumentCaptor.forClass(DefaultExecuteResultHandler.class);

        when(resultHandler.getException()).thenReturn(null);
        when(resultHandler.getExitValue()).thenReturn(0);
        when(resultHandler.hasResult()).thenReturn(true);

        cloneManagerSpy.clone(sourceDatabase, "sourceSchema", targetDatabase, "targetPassword");

        verify(executor).execute(commandLineArgumentCaptor.capture(), defaultExecuteResultHandlerArgumentCaptor.capture());
        assertEquals(commandLineArgumentCaptor.getValue().toString(), expectedCommand);
    }

    @Test(expected = CloneException.class)
    public void cloneExceptionTest() throws Exception {

        when(resultHandler.getException()).thenReturn(new ExecuteException(null, 7));
        when(resultHandler.getExitValue()).thenReturn(0);
        when(resultHandler.hasResult()).thenReturn(true);

        cloneManagerSpy.clone(sourceDatabase, "sourceSchema", targetDatabase, "targetPassword");
    }

    @Test(expected = CloneException.class)
    public void cloneExitValueTest() throws Exception {

        when(resultHandler.getException()).thenReturn(null);
        when(resultHandler.getExitValue()).thenReturn(1);
        when(resultHandler.hasResult()).thenReturn(true);

        cloneManagerSpy.clone(sourceDatabase, "sourceSchema", targetDatabase, "targetPassword");
    }

    @Test(expected = CloneException.class)
    public void cloneNoResultTest() throws Exception {

        when(resultHandler.getException()).thenReturn(null);
        when(resultHandler.getExitValue()).thenReturn(0);
        when(resultHandler.hasResult()).thenReturn(false);

        cloneManagerSpy.clone(sourceDatabase, "sourceSchema", targetDatabase, "targetPassword");
    }
}