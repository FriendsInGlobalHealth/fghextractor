package tz.co.juutech.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/21/21.
 */
public class TableCopierTask implements Callable<Void> {
    private String table;
    private Connection connection;
    private String copyingSql;
    private static final Logger LOGGER = LoggerFactory.getLogger(TableCopierTask.class);

    public TableCopierTask(String table, Connection connection, String copyingSql) {
        assert table != null;
        assert connection != null;
        this.table = table;
        this.connection = connection;
        this.copyingSql = copyingSql;
    }

    public TableCopierTask(String table, Connection connection) {
        assert table != null;
        assert connection != null;
        this.table = table;
        this.connection = connection;
    }

    @Override
    public Void call() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("set foreign_key_checks=0");
            StringBuilder createTableSql = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                    .append(AppProperties.getInstance().getNewDatabaseName())
                    .append(".").append(this.table).append(" LIKE ")
                    .append(AppProperties.getInstance().getDatabaseName())
                    .append(".").append(this.table);

            statement.execute(createTableSql.toString());
            if(this.copyingSql != null) {
                statement.execute(this.copyingSql);
            } else {
                StringBuilder moveRecords = new StringBuilder("INSERT INTO ")
                        .append(AppProperties.getInstance().getNewDatabaseName())
                        .append(".").append(this.table).append(" (SELECT * FROM ").append(AppProperties.getInstance().getDatabaseName())
                        .append(".").append(this.table).append(")");
                this.copyingSql = moveRecords.toString();
                statement.execute(moveRecords.toString());
            }
            LOGGER.debug("Table: " + this.table + ", SQL: "+ copyingSql);
            connection.commit();
            LOGGER.debug("Done copying records for table " + this.table);
            return null;
        } catch (SQLException e) {
            LOGGER.error("SQL during error: {}", this.copyingSql, e);
            throw e;
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException e){}
            }
        }
    }
}
