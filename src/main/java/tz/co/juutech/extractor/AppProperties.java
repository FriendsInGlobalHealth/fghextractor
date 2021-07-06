package tz.co.juutech.extractor;

import com.mysql.jdbc.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/21/21.
 */
public class AppProperties {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppProperties.class);

    public final static String JDBC_URL_PROP = "jdbc.url";
    public final static String DB_NAME_PROP = "db.name";
    public final static String DB_PASSWORD_PROP = "db.password";
    public final static String DB_USERNAME_PROP = "db.username";
    public final static String ENCOUNTER_TYPE_ID_PROP = "encounterType.id";
    public final static String LOCATION_ID_PROP = "location.id";
    public final static String NEW_DB_NAME_PROP = "newDb.name";
    public final static String EXCLUDED_TABLES_PROP = "excluded.tables";
    public final static String ONLY_STRUCTURE_TABLES_PROP = "copy.only.structure";
    public final static String DROP_NEW_DB_AFTER_PROP = "drop.newDb.after";
    public final static String APPLICATION_PROPERTIES_FILENAME = "application.properties";
    public final static String DEV_APPLICATION_PROPERTIES_FILENAME = "dev-application.properties";

    private static AppProperties appProperties = null;
    private static final Properties APP_PROPS = new Properties();
    private Integer encounterTypeId;
    private Integer locationId;
    private String host;
    private String databaseName;
    private Integer port;

    private Boolean dropNewDbAfter;

    private Set<String> excludedTables = new HashSet<>();
    private Set<String> onlyStructureTables = new HashSet<>();

    private AppProperties() {}

    public static AppProperties getInstance() {
        if(appProperties == null) {
            appProperties = new AppProperties();

            try {
                APP_PROPS.load(AppProperties.class.getResourceAsStream(File.separator.concat(APPLICATION_PROPERTIES_FILENAME)));

                InputStream devResourceStream = AppProperties.class.getResourceAsStream(File.separator.concat(DEV_APPLICATION_PROPERTIES_FILENAME));
                if(devResourceStream != null) {
                    APP_PROPS.load(devResourceStream);
                }

                // Apply the custom ones from user
                applyPropertiesFromUser();

                appProperties.encounterTypeId = Integer.valueOf(APP_PROPS.getProperty(ENCOUNTER_TYPE_ID_PROP));
                appProperties.locationId = Integer.valueOf(APP_PROPS.getProperty(LOCATION_ID_PROP));
                appProperties.dropNewDbAfter = Boolean.valueOf(APP_PROPS.getProperty(DROP_NEW_DB_AFTER_PROP, "FALSE"));
                String onlyStructureTables = APP_PROPS.getProperty(ONLY_STRUCTURE_TABLES_PROP, "");
                if(!StringUtils.isNullOrEmpty(onlyStructureTables)) {
                    String[] explodedList = onlyStructureTables.split(",");
                    appProperties.onlyStructureTables = Arrays.stream(explodedList).map(excludedTable -> excludedTable.trim()).collect(Collectors.toSet());
                }

                //Host and port
                appProperties.determineMysqlHostAndPortFromJdbcUrl();
            } catch (Exception e) {
                LOGGER.error("An error occured during reading of app properties (this most likely is a result of invalid application.properties file or lack thereof)");
                LOGGER.error("The passed properties are: {} ", APP_PROPS, e);
            }
        }

        return appProperties;
    }

    public String getJdbcUrl() {
        return APP_PROPS.getProperty(JDBC_URL_PROP);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDbUsername() {
        return APP_PROPS.getProperty(DB_USERNAME_PROP);
    }

    public String getDbPassword() {
        return APP_PROPS.getProperty(DB_PASSWORD_PROP);
    }

    public Integer getEncounterTypeId() {
        return encounterTypeId;
    }

    public Integer getLocationId() {
        return locationId;
    }

    public String getNewDatabaseName() {
        return APP_PROPS.getProperty(NEW_DB_NAME_PROP);
    }

    public Set<String> getExcludedTables() {
        return excludedTables;
    }

    public Set<String> getOnlyStructureTables() {
        return onlyStructureTables;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public Boolean getDropNewDbAfter() {
        return dropNewDbAfter;
    }

    @Override
    public String toString() {
        return APP_PROPS.toString();
    }

    private void determineMysqlHostAndPortFromJdbcUrl() {
        String jdbcUrl = this.getJdbcUrl();
        if(jdbcUrl != null) {
            int indexOfDoubleSlash = jdbcUrl.indexOf("//");
            String hostToEnd = jdbcUrl.substring(indexOfDoubleSlash + 2);
            int indexOfColonAfterHost = hostToEnd.indexOf(':');
            int indexOfSlashAfterHost = hostToEnd.indexOf('/');
            if (indexOfColonAfterHost > 0) {
                this.host = hostToEnd.substring(0, indexOfColonAfterHost);
                this.port = Integer.valueOf(hostToEnd.substring(indexOfColonAfterHost + 1, indexOfSlashAfterHost));

            } else {
                this.host = hostToEnd.substring(0, indexOfSlashAfterHost);
                this.port = 3306;
            }

            int indexOfQuestionMark = hostToEnd.indexOf('?');
            if(indexOfQuestionMark > 0) {
                this.databaseName = hostToEnd.substring(indexOfSlashAfterHost + 1, indexOfQuestionMark);
            } else {
                this.databaseName = hostToEnd.substring(indexOfSlashAfterHost);
            }
        }
    }

    private static Properties applyPropertiesFromUser() {
        String dir = System.getProperty("user.dir");
        String filePath = null;
        if(dir.endsWith(File.separator)) {
            filePath = dir + APPLICATION_PROPERTIES_FILENAME;
        } else {
            filePath = dir + File.separator + APPLICATION_PROPERTIES_FILENAME;
        }

        File userFile = new File(filePath);

        if(userFile.exists() && userFile.isFile()) {
            Properties userProps = new Properties();
            try {
                userProps.load(new FileInputStream(userFile));

                if(!userProps.isEmpty()) {
                    APP_PROPS.putAll(userProps);
                }
            } catch (FileNotFoundException e) {
                LOGGER.warn("If you are running this as a jar you need to provide an application.properties file with required minimum configuration", e);
            } catch (IOException e) {
                LOGGER.error("Oops! Problems loading file: {}", userFile.getAbsolutePath(), e);
            }
        }

        return APP_PROPS;
    }

    class ObsProperty {
        Integer conceptId;
        String valueColumn;
        String value;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ObsProperty)) return false;

            ObsProperty that = (ObsProperty) o;

            if (!conceptId.equals(that.conceptId)) return false;
            if (!valueColumn.equals(that.valueColumn)) return false;
            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            int result = conceptId.hashCode();
            result = 31 * result + valueColumn.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }
    }
}
