package tz.co.juutech.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/21/21.
 */
public class ExtractionUtils {
    private static List<String> tablesToMove = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractionUtils.class);

    public static List<String> getListofTablesToMove(Connection connection) throws SQLException {
        if(tablesToMove == null) {
            tablesToMove = new ArrayList<>();
            try {
                ResultSet rs = connection.getMetaData().getTables(null, AppProperties.getInstance().getDatabaseName(), null, null);

                while (rs.next()) {
                    tablesToMove.add(rs.getString("TABLE_NAME"));
                }
            } catch (SQLException e){
                LOGGER.error("An error while all tables from the source db {} ", AppProperties.getInstance().getDatabaseName(), e);
                throw e;
            } finally {
                if(connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e){}
                }
            }
            // Remove excluded tables if any
            if (AppProperties.getInstance().getExcludedTables().isEmpty()) {
                tablesToMove.removeAll(AppProperties.getInstance().getExcludedTables());
            }

            // Remove patient & encounter
            tablesToMove.remove("encounter");
            tablesToMove.remove("patient");
            tablesToMove.remove("person");
            tablesToMove.remove("users");

            // Remove tables which we are only to copy structure
            tablesToMove.removeAll(AppProperties.getInstance().getOnlyStructureTables());
        }
        return tablesToMove;
    }

    public static String getCopyingSQL(final String table, final String condition) {
        StringBuilder sql = new StringBuilder("INSERT INTO ")
                .append(AppProperties.getInstance().getNewDatabaseName())
                .append(".").append(table).append(" (SELECT * FROM ").append(AppProperties.getInstance().getDatabaseName())
                .append(".").append(table).append(" AS t WHERE ").append(condition).append(")");
        return sql.toString();
    }

    public static void createNewDatabase(final Connection connection) throws SQLException {
        String sql = "CREATE DATABASE IF NOT EXISTS " + AppProperties.getInstance().getNewDatabaseName();
        try (Statement s = connection.createStatement()) {
            s.execute(sql);
        } catch (SQLException sqle) {
            LOGGER.error("An error occured while running sql: {}", sql, sqle);
            throw sqle;
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException e){}
            }
        }
    }

    public static void copyOnlyStructure(final Connection connection, final Set<String> tables) throws SQLException {
        try {
            for (String table : tables) {
                copyOnlyStructure(connection, table, false);
            }
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException e){}
            }
        }
    }

    public static void copyOnlyStructure(final Connection connection, final String table, final boolean closeConnection) throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE ").append(AppProperties.getInstance().getNewDatabaseName()).append(".")
                .append(table).append(" AS SELECT * FROM ").append(AppProperties.getInstance().getDatabaseName()).append(".").append(table)
                .append(" LIMIT 0");
        try (Statement s = connection.createStatement()) {
            s.execute(sql.toString());
        } catch (SQLException sqle) {
            LOGGER.error("An error occured while running sql: {}", sql.toString(), sqle);
            throw sqle;
        } finally {
            if(closeConnection && connection != null) {
                try {
                    connection.close();
                } catch (SQLException e){}
            }
        }
    }

    /**
     *
     * @param integers set of integer to be transformed into a set acceptable in SQL for example "(4,5,98,71)"
     * @return String
     */
    public static String stringifySetOfIntegers(Set<Integer> integers) {
        StringBuilder sb = new StringBuilder("(");
        integers.forEach(integer -> sb.append(integer).append(","));
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        return sb.toString();
    }

    public static String getDumpFilename() {
        return new StringBuilder(AppProperties.getInstance().getNewDatabaseName()).append(".")
                .append(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME)).append(".sql").toString();
    }
}
