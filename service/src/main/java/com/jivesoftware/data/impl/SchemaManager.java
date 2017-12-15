package com.jivesoftware.data.impl;

import com.jivesoftware.data.exceptions.SchemaOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.sql.*;
import java.util.Properties;

public class SchemaManager {
    private final static Logger logger = LoggerFactory.getLogger(SchemaManager.class);

    private static final String SUPERUSER = "postgres";

    private static final String CREATE_USER_SQL = "CREATE USER %s CREATEDB;";
    private static final String ASSIGN_PASSWORD_SQL = "ALTER USER %s PASSWORD '%s';";
    private static final String GRANT_ROLE_SQL = "GRANT %s TO %s";
    private static final String CREATE_DATABASE_SQL = "CREATE DATABASE %s OWNER = %s;";
    private static final String ALTER_PERMS_SQL = "ALTER DEFAULT PRIVILEGES FOR ROLE %s GRANT SELECT ON TABLES TO %s;";
    private static final String CHECK_SCHEMA_EXISTS =
            "SELECT EXISTS(SELECT datname FROM pg_database WHERE datname = '%s')";
    private static final String DROP_DATABASE_SQL = "DROP DATABASE %s;";
    private static final String DROP_USER_SQL = "DROP USER %s;";
    private static final String CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS %s;";
    private static final String CREATE_MQ_USER_SQL =    "DO\n" +
                                                        "$$\n" +
                                                        "BEGIN\n" +
                                                        "   IF NOT EXISTS (\n" +
                                                        "      SELECT *\n" +
                                                        "      FROM   pg_catalog.pg_user\n" +
                                                        "      WHERE  usename = 'mq2user') THEN\n" +
                                                        "      CREATE ROLE mq2user LOGIN PASSWORD '%s' valid until '%s';\n" +
                                                        "      GRANT SELECT,DELETE,UPDATE ON ALL TABLES IN SCHEMA PUBLIC TO mq2user;\n" +
                                                        "   ELSE\n" +
                                                        "      ALTER ROLE mq2user LOGIN PASSWORD '%s' valid until '%s';\n" +
                                                        "      GRANT SELECT,DELETE,UPDATE ON ALL TABLES IN SCHEMA PUBLIC TO mq2user;\n" +
                                                        "   END IF;\n" +
                                                        "END\n" +
                                                        "$$;";

    public void createSchema(@NotNull MasterDatabase masterDatabase,
                             @NotNull String schemaName,
                             @NotNull String schemaUser,
                             @NotNull String schemaPassword) {
        try (Connection connection = getConnection(masterDatabase, masterDatabase.getSchema());
             Statement statement = connection.createStatement()) {
            logger.debug(String.format("Creating %s on %s owned by %s",
                    schemaName, masterDatabase.getHost(), schemaUser));
            statement.executeUpdate(String.format(CREATE_USER_SQL, schemaUser));
            statement.executeUpdate(String.format(ASSIGN_PASSWORD_SQL, schemaUser,
                    schemaPassword));
            statement.executeUpdate(String.format(GRANT_ROLE_SQL, schemaName, SUPERUSER));
            statement.executeUpdate(String.format(CREATE_DATABASE_SQL, schemaName, schemaUser));
            statement.executeUpdate(String.format(ALTER_PERMS_SQL, schemaUser, schemaUser));
            logger.debug(String.format("%s created successfully", schemaName));
        } catch (SQLException e) {
            logger.error(String.format("There was a SQL exception creating schema %s", schemaName), e);

            throw new SchemaOperationException(e.getMessage());
        }
        try (Connection connectionToNew = getConnection(masterDatabase, schemaName);
             Statement statement = connectionToNew.createStatement()) {
            logger.debug(String.format("Attempting to add extensions for %s", schemaName));
            statement.executeUpdate(String.format(CREATE_EXTENSION, "hstore"));
            statement.executeUpdate(String.format(CREATE_EXTENSION, "pg_buffercache"));
            statement.executeUpdate(String.format(CREATE_EXTENSION, "pg_stat_statements"));
            statement.executeUpdate(String.format(CREATE_EXTENSION, "pgstattuple"));
            statement.executeUpdate(String.format(CREATE_EXTENSION, "postgres_fdw"));
        } catch (SQLException e) {
            logger.error(String.format("Error adding extensions %s", schemaName), e);

            throw new SchemaOperationException(e.getMessage());
        }
    }

    public void createUser(@NotNull MasterDatabase database,
                           @NotNull String schemaName,
                           @NotNull String schemaPassword,
                           @NotNull String validUntil) {
        try (Connection connection = getConnection(database, schemaName);
             Statement statement = connection.createStatement()) {
             statement.executeUpdate(String.format(CREATE_MQ_USER_SQL, schemaPassword,validUntil,schemaPassword,validUntil));
        } catch (SQLException e) {
            logger.error(String.format("Error creating mq2user"), e);
            throw new SchemaOperationException(e.getMessage());
        }
    }

    public void changeSchemaPassword(@NotNull MasterDatabase database,
                                     @NotNull String schemaName,
                                     @NotNull String schemaUser,
                                     @NotNull String password) {
        try (Connection connection = getConnection(database, database.getSchema());
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format(ASSIGN_PASSWORD_SQL, schemaUser, password));
        } catch (SQLException e) {
            logger.error(String.format("Error revoking access to schema %s", schemaName), e);
            throw new SchemaOperationException(e.getMessage());
        }
    }

    public boolean isSchemaExists(@NotNull MasterDatabase database,
                                  @NotNull String schemaName)  {
        try (Connection connection = getConnection(database, database.getSchema());
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(String.format(CHECK_SCHEMA_EXISTS, schemaName))) {
            return resultSet.next() && resultSet.getBoolean(1);
        } catch (SQLException e) {
            logger.error(String.format("Error checking if schema %s exists", schemaName), e);
            throw new SchemaOperationException(e.getMessage());
        }
    }

    Connection getConnection(@NotNull MasterDatabase database, String databaseName) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", database.getUsername());
        props.setProperty("password", database.getPassword());
        String connectionURL = String.format("jdbc:postgresql://%s:%d/%s",
                database.getHost(), database.getPort(), databaseName);
        logger.debug(String.format("Attempting to connect to %s as user %s on %s",
                databaseName, database.getUsername(), connectionURL));
        return DriverManager.getConnection(connectionURL, props);
    }


    public void hardDeleteSchema(@NotNull MasterDatabase database, String userName, String schemaName) {
        try (Connection connection = getConnection(database, database.getSchema());
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format(DROP_DATABASE_SQL, schemaName));
            statement.executeUpdate(String.format(DROP_USER_SQL, userName));
        } catch (SQLException e) {
            logger.error(String.format("Error deleting database %s", database.getSchema()), e);
            throw new SchemaOperationException(e.getMessage());
        }
    }

}
