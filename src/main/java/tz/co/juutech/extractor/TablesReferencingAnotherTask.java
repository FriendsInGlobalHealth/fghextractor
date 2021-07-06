package tz.co.juutech.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import static tz.co.juutech.extractor.ExtractionUtils.getListofTablesToMove;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/22/21.
 */
public class TablesReferencingAnotherTask implements Callable<Set<TableReferencingAnother>> {
    private Connection connection;
    private String parentTable;
    private String pkColumn;
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public TablesReferencingAnotherTask(final Connection connection, final String parentTable, final String pkColumn) {
        assert connection != null;
        assert parentTable != null;
        assert pkColumn != null;
        this.connection = connection;
        this.parentTable = parentTable;
        this.pkColumn = pkColumn;
    }

    @Override
    public Set<TableReferencingAnother> call() throws Exception {
        return getTablesReferencingAnother();
    }

    private Set<TableReferencingAnother> getTablesReferencingAnother() throws SQLException {
        Set<TableReferencingAnother> tablesReferencingAnother = new HashSet<>();
        try {
            List<String> tablesToBeMoved = getListofTablesToMove(connection);
            if (!tablesToBeMoved.isEmpty()) {
                final DatabaseMetaData DB_MD = connection.getMetaData();
                final String DB_NAME = AppProperties.getInstance().getDatabaseName();
                for (String tableName : tablesToBeMoved) {
                    ResultSet references = DB_MD.getCrossReference(null, DB_NAME, parentTable, null, DB_NAME, tableName);
                    while (references.next()) {
                        if (references.getString("PKCOLUMN_NAME").contentEquals(pkColumn)) {
                            tablesReferencingAnother.
                                    add(new TableReferencingAnother(references.getString("FKTABLE_NAME"), references.getString("FKCOLUMN_NAME")));
                        }
                    }
                }
            }
            return tablesReferencingAnother;
        } catch (SQLException sqle) {
            logger.error("Error while finding tables referencing {} ({})", this.parentTable, this.pkColumn, sqle);
            throw sqle;
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException e){}
            }
        }
    }
}
