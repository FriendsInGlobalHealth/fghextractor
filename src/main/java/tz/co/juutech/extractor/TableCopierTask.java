package tz.co.juutech.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/21/21.
 */
public class TableCopierTask implements Callable<Void> {
    private String table;
    private String condition;
    private static final Logger LOGGER = LoggerFactory.getLogger(TableCopierTask.class);

    public TableCopierTask(String table, String condition) {
        assert table != null;
        this.table = table;
        this.condition = condition;
    }

    public TableCopierTask(String table) {
        assert table != null;
        this.table = table;
    }

    @Override
    public Void call() throws SQLException {
        ResultSet resultSet = null;
        try (   Connection connection = ConnectionPool.getConnection();
                Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("set foreign_key_checks=0");
            ExtractionUtils.copyOnlyStructure(this.table);

            resultSet = statement.executeQuery(ExtractionUtils.getCountingQuery(table, condition));
            resultSet.next();
            int countToMove = resultSet.getInt(1);
            int batchSize = AppProperties.getInstance().getBatchSize();
            if(countToMove > batchSize) {
                LOGGER.trace("Copying {} records from {} in batches of {}", countToMove, this.table, batchSize);
                int start = 0;
                int temp = countToMove;
                String copyingSql;
                // TODO: Assuming the openmrs convention where primary key column is always table_id, need to find a robust way of obtaining this.
                String orderColumn = table + "_id";
                int batchCount = 1;
                int totalCopied = 0;
                while (temp % batchSize > 0) {
                    if (temp / batchSize > 0) {
                        LOGGER.trace("Copying batch # {} of {} table, records copied:  {}, remaining: {}", batchCount++, this.table, totalCopied,
                                countToMove - totalCopied);
                        copyingSql = ExtractionUtils.getCopyingSQLWithOrderAndPaging(table, condition, orderColumn, start, batchSize);
                        temp -= batchSize;
                        totalCopied += batchSize;
                    } else {
                        LOGGER.trace("Copying batch # {} of {} table, records copied:  {}, remaining: {}", batchCount++, this.table, totalCopied,
                                countToMove - totalCopied);
                        copyingSql = ExtractionUtils.getCopyingSQLWithOrderAndPaging(table, condition, orderColumn, start, temp);
                        temp = 0;
                        totalCopied += temp;
                    }
                    start += batchSize;
                    LOGGER.debug("Running SQL statement: {}", copyingSql);
                    statement.execute(copyingSql);
                }
            } else {
                // few records to move.
                String copyingSql = ExtractionUtils.getCopyingSQL(table, condition);
                LOGGER.debug("Running SQL statement: {}", copyingSql);
                statement.execute(copyingSql);
            }

            connection.commit();
            LOGGER.debug("Done copying {} records for table {}", countToMove, this.table);
            return null;
        } catch (SQLException e) {
            LOGGER.error("An error has occured while copying records for table {}", this.table, e);
            throw e;
        } finally {
            if(resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e){}
            }
        }
    }
}
