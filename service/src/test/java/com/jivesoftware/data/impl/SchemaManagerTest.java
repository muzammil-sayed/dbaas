package com.jivesoftware.data.impl;

import com.jivesoftware.data.exceptions.SchemaOperationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SchemaManagerTest {

    private SchemaManager schemaManager = new SchemaManager();

    final SchemaManager mockSchemaManager = spy(schemaManager);

    @Mock
    private MasterDatabase masterDatabase;

    @Mock
    private DriverManager driverManager;

    @Mock
    private Connection superConnection;

    @Mock
    private Connection directConnection;

    @Mock
    private Statement superStatement;

    @Mock
    private Statement directStatement;

    @Mock
    private Properties properties;

    @Mock
    private ResultSet resultSet;


    @Before
    public void setUp() throws SQLException {

        when(masterDatabase.getUsername()).thenReturn("username");
        when(masterDatabase.getPassword()).thenReturn("password");
        when(masterDatabase.getHost()).thenReturn("host");
        when(masterDatabase.getPort()).thenReturn(5432);
        when(masterDatabase.getSchema()).thenReturn("masterDB");
        doReturn(superConnection).when(mockSchemaManager).getConnection(masterDatabase, "masterDB");
        doReturn(directConnection).when(mockSchemaManager)
                .getConnection(masterDatabase, "schemaName");

    }

    @Test
     public void createSchemaTest() throws SQLException {

        when(superConnection.createStatement()).thenReturn(superStatement);
        when(directConnection.createStatement()).thenReturn(directStatement);
        mockSchemaManager.createSchema(masterDatabase, "schemaName", "username", "password");
        verify(superConnection).close();
        verify(superStatement).close();
        verify(superStatement).executeUpdate("CREATE USER username CREATEDB;");
        verify(superStatement).executeUpdate("ALTER USER username PASSWORD 'password';");
        verify(superStatement).executeUpdate("GRANT schemaName TO postgres");
        verify(superStatement).executeUpdate("CREATE DATABASE schemaName OWNER = username;");
        verify(superStatement).executeUpdate(
                "ALTER DEFAULT PRIVILEGES FOR ROLE username GRANT SELECT ON TABLES TO username;");
        verify(directStatement).executeUpdate("CREATE EXTENSION IF NOT EXISTS hstore;");
        verify(directStatement).executeUpdate("CREATE EXTENSION IF NOT EXISTS pg_buffercache;");
        verify(directStatement).executeUpdate("CREATE EXTENSION IF NOT EXISTS pgstattuple;");
        verify(directStatement).executeUpdate("CREATE EXTENSION IF NOT EXISTS postgres_fdw;");
        verify(directStatement).executeUpdate("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;");
    }

    @Test
    public void createUserTest() throws SQLException {

        when(directConnection.createStatement()).thenReturn(directStatement);
        mockSchemaManager.createUser(masterDatabase, "schemaName", "password", "validUntil");
        verify(directConnection).close();
        verify(directStatement).close();
        verify(directStatement).executeUpdate("DO\n" +
                "$$\n" +
                "BEGIN\n" +
                "   IF NOT EXISTS (\n" +
                "      SELECT *\n" +
                "      FROM   pg_catalog.pg_user\n" +
                "      WHERE  usename = 'mq2user') THEN\n" +
                "      CREATE ROLE mq2user LOGIN PASSWORD 'password' valid until 'validUntil';\n" +
                "      GRANT SELECT,DELETE,UPDATE ON ALL TABLES IN SCHEMA PUBLIC TO mq2user;\n" +
                "   ELSE\n" +
                "      ALTER ROLE mq2user LOGIN PASSWORD 'password' valid until 'validUntil';\n" +
                "      GRANT SELECT,DELETE,UPDATE ON ALL TABLES IN SCHEMA PUBLIC TO mq2user;\n" +
                "   END IF;\n" +
                "END\n" +
                "$$;");

    }

    @Test
    public void deleteSchemaTest() throws SQLException {

        when(superConnection.createStatement()).thenReturn(superStatement);

        mockSchemaManager.changeSchemaPassword(masterDatabase, "schemaName", "username", "password");
        verify(superConnection).close();
        verify(superStatement).close();
        verify(superStatement).executeUpdate("ALTER USER username PASSWORD 'password';");
    }

    @Test
    public void isSchemaExistsTest() throws SQLException {

        when(superConnection.createStatement()).thenReturn(superStatement);
        when(superStatement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(true);

        assertTrue(mockSchemaManager.isSchemaExists(masterDatabase, "schemaName"));
        verify(superConnection).close();
        verify(superStatement).close();
        verify(superStatement).executeQuery(
                "SELECT EXISTS(SELECT datname FROM pg_database WHERE datname = 'schemaName')");
    }

    @Test
    public void hardDeleteSchemaTest() throws SQLException {

        when(superConnection.createStatement()).thenReturn(superStatement);

        mockSchemaManager.hardDeleteSchema(masterDatabase, "username", "schemaName");
        verify(superConnection).close();
        verify(superStatement).close();
        verify(superStatement).executeUpdate("DROP DATABASE schemaName;");
        verify(superStatement).executeUpdate("DROP USER username;");
    }

    @Test(expected = SchemaOperationException.class)
    public void createSchemaExceptionTest() throws SQLException {

        when(superConnection.createStatement()).thenThrow(SQLException.class);
        mockSchemaManager.createSchema(masterDatabase, "schemaName", "username", "password");

    }

    @Test(expected = SchemaOperationException.class)
    public void deleteSchemaExceptionTest() throws SQLException {

        when(superConnection.createStatement()).thenThrow(SQLException.class);
        mockSchemaManager.changeSchemaPassword(masterDatabase, "schemaName", "username", "password");

    }

    @Test(expected = SchemaOperationException.class)
    public void isSchemaExistsExceptionTest() throws SQLException {

        when(superConnection.createStatement()).thenThrow(SQLException.class);
        mockSchemaManager.isSchemaExists(masterDatabase, "schemaName");


    }

    @Test(expected = SchemaOperationException.class)
    public void createUserExceptionTest() throws SQLException {

        when(directConnection.createStatement()).thenThrow(SQLException.class);
        mockSchemaManager.createUser(masterDatabase, "schemaName", "password", "validUntil");

    }

    @Test(expected = SchemaOperationException.class)
    public void hardDeleteSchemaExpectionTest() throws SQLException {

        when(superConnection.createStatement()).thenThrow(SQLException.class);
        mockSchemaManager.hardDeleteSchema(masterDatabase, "username", "schemaName");


    }
}






