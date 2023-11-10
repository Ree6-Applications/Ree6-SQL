package de.presti.ree6.sql;

import com.zaxxer.hikari.HikariDataSource;
import io.sentry.Sentry;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.output.MigrateResult;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reflections.util.ClasspathHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Map;

/**
 * A "Connector" Class which connect with the used Database Server.
 * Used to manage the connection between Server and Client.
 */
@Slf4j
@SuppressWarnings({"SqlNoDataSourceInspection", "SqlResolve"})
public class SQLConnector {


    /**
     * An Instance of the actual Java SQL Connection.
     */
    @Getter
    private HikariDataSource dataSource;

    /**
     * An Instance of the SQL-Worker which works with the Data in the Database.
     */
    @Getter
    private final SQLWorker sqlWorker;

    /**
     * A boolean to keep track if there was at least one valid connection.
     */
    private boolean connectedOnce = false;


    /**
     * A boolean to keep track if there was at least two valid connections.
     */
    @Setter
    private boolean connectedSecond = false;

    /**
     * Constructor with the needed data to open an SQL connection.
     */
    public SQLConnector() {
        sqlWorker = new SQLWorker(this);

        connectToSQLServer();
        runMigrations();
    }

    /**
     * Try to open a connection to the SQL Server with the given data.
     */
    public void connectToSQLServer() {
        log.info("Connecting to SQl-Service (SQL).");
        // Check if there is already an open Connection.
        if (isConnected()) {
            try {
                // Close if there is and notify.
                getDataSource().close();
                log.info("Service (SQL) has been stopped.");
            } catch (Exception ignore) {
                // Notify if there was an error.
                log.error("Service (SQL) couldn't be stopped.");
            }
        }

        try {
            dataSource = new HikariDataSource(SQLSession.buildHikariConfig());
            log.info("Service (SQL) has been started. Connection was successful.");
            connectedOnce = true;
        } catch (Exception exception) {
            // Notify if there was an error.
            log.error("Service (SQL) couldn't be started. Connection was unsuccessful.", exception);
        }
    }

    /**
     * Run all Flyway Migrations.
     */
    public void runMigrations() {

        // Check if there is an open Connection if not, skip.
        if (!isConnected()) return;

        try {
            Flyway flyway = Flyway
                    .configure(ClasspathHelper.staticClassLoader())
                    .dataSource(getDataSource())
                    .locations("sql/migrations")
                    .installedBy("Ree6-SQL").load();

            MigrationInfo[] migrationInfo = flyway.info().pending();

            if (migrationInfo.length != 0) {
                log.info("Found " + migrationInfo.length + " pending migrations.");
                log.info("The pending migrations are: " + String.join(", ",
                        Arrays.stream(migrationInfo).map(MigrationInfo::getDescription).toArray(String[]::new)));

                log.info("Running Flyway Migrations.");
            } else {
                log.info("No pending migrations found.");
            }

            MigrateResult result = flyway.migrate();

            if (result.success) {
                log.info("Flyway Migrations were successful.");
            } else {
                log.error("Flyway Migrations were unsuccessful.");
            }
        } catch (Exception exception) {
            log.error("Migration failed!", exception);
        }
    }

    //region Utility

    /**
     * Query basic SQL Statements, without using the ORM-System.
     *
     * @param sqlQuery   The SQL Query.
     * @param parameters The Parameters for the Query.
     * @return Either a {@link Integer} or the result object of the ResultSet.
     */
    public Object querySQL(String sqlQuery, Object... parameters) {
        if (!isConnected()) {
            if (connectedOnce()) {
                connectToSQLServer();
                return querySQL(sqlQuery, parameters);
            } else {
                return null;
            }
        }

        try (Connection connection = getDataSource().getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            for (int i = 0; i < parameters.length; i++) {
                preparedStatement.setObject(i + 1, parameters[i]);
            }
            if (sqlQuery.startsWith("SELECT")) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    return resultSet.next();
                }
            } else {
                return preparedStatement.executeUpdate();
            }
        } catch (Exception exception) {
            Sentry.captureException(exception);
        }

        return null;
    }

    /**
     * Send an SQL-Query to SQL-Server and get the response.
     *
     * @param <R>        The Class entity.
     * @param r          The class entity.
     * @param sqlQuery   the SQL-Query.
     * @param parameters a list with all parameters that should be considered.
     * @return The Result from the SQL-Server.
     */
    public <R> Query<R> querySQL(@NotNull R r, @NotNull String sqlQuery, @Nullable Map<String, Object> parameters) {

        if (!isConnected()) {
            if (connectedOnce()) {
                connectToSQLServer();
                return querySQL(r, sqlQuery, parameters);
            } else {
                return null;
            }
        }

        try (Session session = SQLSession.getSessionFactory().openSession()) {

            session.beginTransaction();

            Query<R> query = (Query<R>) session.createNativeQuery(sqlQuery, r.getClass());

            if (parameters != null) {
                for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                    query.setParameter(entry.getKey(), entry.getValue());
                }
            }

            session.getTransaction().commit();

            return query;
        } catch (Exception exception) {
            Sentry.captureException(exception);
            throw exception;
        }
    }

    //endregion

    /**
     * Check if there is an open connection to the Database Server.
     *
     * @return boolean If the connection is opened.
     */
    public boolean isConnected() {
        try {
            return getDataSource() != null && !getDataSource().isClosed();
        } catch (Exception ignore) {
        }

        return false;
    }

    /**
     * Call to close the current Connection.
     */
    public void close() {
        // Check if there is already an open Connection.
        if (isConnected()) {
            try {
                // Close if there is and notify.
                getDataSource().close();
                log.info("Service (SQL) has been stopped.");
            } catch (Exception ignore) {
                // Notify if there was an error.
                log.error("Service (SQL) couldn't be stopped.");
            }
        }
    }

    /**
     * Check if there was at least one successful Connection to the Database Server.
     *
     * @return boolean If there was at least one successful Connection.
     */
    public boolean connectedOnce() {
        return connectedOnce;
    }

    /**
     * Check if there was at least one successful Connection to the Database Server.
     *
     * @return boolean If there was at least one successful Connection.
     */
    public boolean connectedSecond() {
        return connectedSecond;
    }
}