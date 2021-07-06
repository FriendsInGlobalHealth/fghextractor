package tz.co.juutech.extractor;

import org.apache.commons.dbcp2.cpdsadapter.DriverAdapterCPDS;
import org.apache.commons.dbcp2.datasources.SharedPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/29/21.
 */
public class ConnectionPool {
    public static int MAX_CONNECTIONS = 100;
    private static DataSource ds;
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPool.class);
    static {
        DriverAdapterCPDS cpds = new DriverAdapterCPDS();
        try {
            cpds.setDriver("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException fe) {
            fe.printStackTrace();
        }
        cpds.setUrl(AppProperties.getInstance().getJdbcUrl());
        cpds.setUser(AppProperties.getInstance().getDbUsername());
        cpds.setPassword(AppProperties.getInstance().getDbPassword());
        SharedPoolDataSource tds = new SharedPoolDataSource();
        tds.setConnectionPoolDataSource(cpds); tds.setMaxTotal(MAX_CONNECTIONS);
        ds = tds;
    }

    public static Connection getConnection() throws SQLException {
        try {
            return ds.getConnection();
        } catch (SQLException sqle) {
            LOGGER.error("Error connecting to the database using url: {}, username: {} and password: {}", AppProperties.getInstance().getJdbcUrl(),
                    AppProperties.getInstance().getDbUsername(), AppProperties.getInstance().getDbPassword());
            throw sqle;
        }
    }

}
