package tz.co.juutech.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

            // Remove patient & person
            tablesToMove.remove("patient");
            tablesToMove.remove("person");

            // Remove users related tables
            tablesToMove.remove("users");
            tablesToMove.remove("user_role");
            tablesToMove.remove("user_property");

            // Remove tables which we are only to copy structure
            tablesToMove.removeAll(AppProperties.getInstance().getOnlyStructureTables());
        }
        return tablesToMove;
    }

    public static String getCopyingSQL(final String table, final String condition) {
        StringBuilder sql = new StringBuilder("INSERT INTO ")
                .append(AppProperties.getInstance().getNewDatabaseName())
                .append(".").append(table).append(" (SELECT * FROM ").append(AppProperties.getInstance().getDatabaseName())
                .append(".").append(table);
        if(condition != null) {
            sql.append(" AS t WHERE ").append(condition).append(")");
        }
        return sql.toString();
    }

    public static String getCopyingSQLWithOrderAndPaging(final String table, final String condition, final String orderColumn,
                                                         final Integer startIndex, final Integer size) {
        StringBuilder sql = new StringBuilder("INSERT INTO ")
                .append(AppProperties.getInstance().getNewDatabaseName())
                .append(".").append(table).append(" (SELECT * FROM ").append(AppProperties.getInstance().getDatabaseName())
                .append(".").append(table);
        if(condition != null) {
            sql.append(" AS t WHERE ").append(condition);
        }
        sql.append(" ORDER BY ").append(orderColumn).append(" LIMIT ").append(startIndex).append(", ").append(size).append(")");
        return sql.toString();
    }

    public static String getCountingQuery(final String table, final String condition) {
        StringBuilder countQuery = new StringBuilder("SELECT COUNT(*) FROM ")
                .append(AppProperties.getInstance().getDatabaseName())
                .append(".").append(table);
        if(condition != null ) {
            countQuery.append(" AS t WHERE ").append(condition);
        }
        return countQuery.toString();
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

    public static void copyOnlyStructure(final Set<String> tables) throws SQLException {
        for (String table : tables) {
            copyOnlyStructure(table);
        }
    }

    public static void copyOnlyStructure(final String table) throws SQLException {
        String createTableSql = null;
        try (Connection connection = ConnectionPool.getConnection();
             Statement s = connection.createStatement();
             ResultSet resultSet =
                     s.executeQuery("SHOW CREATE TABLE ".concat(AppProperties.getInstance().getDatabaseName()).concat(".").concat(table))) {
            resultSet.next();
            // The second column is named "Create Table"
            createTableSql = resultSet.getString(2);
            String createInNewDb = "CREATE TABLE IF NOT EXISTS ".concat(AppProperties.getInstance().getNewDatabaseName()).concat(".");
            createTableSql = createTableSql.replace("CREATE TABLE ", createInNewDb);
            s.execute("set foreign_key_checks=0");
            s.execute(createTableSql);
        } catch (SQLException sqle) {
            LOGGER.error("An error occured while running sql: {}", createTableSql, sqle);
            throw sqle;
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

    public static String getPatientListQueryFromFile() throws URISyntaxException, IOException {
        URL sqlFileUrl = FGHExtractorOrchestrator.class.getResource(File.separator.concat(AppProperties.getInstance().getPatientListQueryFileName()));
        return Files.lines(Paths.get(sqlFileUrl.toURI())).collect(Collectors.joining(" "))
                .replace("sourceDatabase", AppProperties.getInstance().getDatabaseName())
                .replace(":location", AppProperties.getInstance().getLocationId().toString())
                .replace(":endDate", String.format("'%s'", AppProperties.getInstance().getFormattedEndDate("yyyy-MM-dd")));
    }

    public static String getDumpFilename() {
        return new StringBuilder(AppProperties.getInstance().getNewDatabaseName()).append(".")
                .append(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME)).append(".sql").toString();
    }
}
