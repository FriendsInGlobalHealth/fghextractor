package tz.co.juutech.extractor;

import com.mysql.jdbc.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tz.co.juutech.extractor.exception.InvalidMandatoryPropertyValueException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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
    public final static String DB_PASSWORD_PROP = "db.password";
    public final static String DB_USERNAME_PROP = "db.username";
    public final static String LOCATIONS_IDS_PROP = "locations.ids";
    public final static String END_DATE_PROP = "end.date";
    public final static String END_DATE_PATTERN_PROP = "end.date.pattern";
    public final static String NEW_DB_NAME_PROP = "newDb.name";
    public final static String EXCLUDED_TABLES_PROP = "excluded.tables";
    public final static String ONLY_STRUCTURE_TABLES_PROP = "copy.only.structure";
    public final static String DROP_NEW_DB_AFTER_PROP = "drop.newDb.after";
    public final static String BATCH_SIZE_PROP = "batch.size";
    public final static String APPLICATION_PROPERTIES_FILENAME = "application.properties";
    public final static String DEV_APPLICATION_PROPERTIES_FILENAME = "dev-application.properties";
    public static final String LOG_LEVEL_PROP = "log.level";

    private static final String PATIENT_LIST_QUERY_FILE = "patient_list_query.sql";
    private static final String DEFAULT_END_DATE_PATTERN = "dd-MM-yyyy";
    private static final int DEFAULT_BATCH_SIZE = 20000;

    private static AppProperties appProperties = null;
    private static final Properties APP_PROPS = new Properties();
    private String locationsIdsString;
    private Set<Integer> locationsIds = new HashSet<>();
    private LocalDate endDate;
    private DateTimeFormatter endDateFormatter;
    private String host;
    private String databaseName;
    private Integer port;

    private Boolean dropNewDbAfter;
    private Integer batchSize;

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

                String[] locIds = APP_PROPS.getProperty(LOCATIONS_IDS_PROP).split(",");
                for(String locId: locIds) {
                    try {
                        appProperties.locationsIds.add(Integer.parseInt(locId.trim()));
                    } catch (NumberFormatException e) {
                        LOGGER.error("The provided location Id {} is invalid, please use numbers", locId);
                        throw new InvalidMandatoryPropertyValueException(LOCATIONS_IDS_PROP, APP_PROPS.getProperty(LOCATIONS_IDS_PROP));
                    }
                }

                appProperties.locationsIdsString = APP_PROPS.getProperty(LOCATIONS_IDS_PROP);
                try {
                    String datePattern = APP_PROPS.getProperty(END_DATE_PATTERN_PROP, DEFAULT_END_DATE_PATTERN);
                    appProperties.endDateFormatter = DateTimeFormatter.ofPattern(datePattern);
                    appProperties.endDate = LocalDate.parse(APP_PROPS.getProperty(END_DATE_PROP), appProperties.endDateFormatter);
                } catch (DateTimeParseException|NullPointerException e) {
                    if(APP_PROPS.containsKey(END_DATE_PROP) && APP_PROPS.getProperty(END_DATE_PROP) != null) {
                        LOGGER.error("Invalid value set for property {}", END_DATE_PROP);
                        throw e;
                    } else {
                        // Just use now.
                        LOGGER.info("{} property not set, defaulting to current date", END_DATE_PROP);
                        appProperties.endDate = LocalDate.now();
                    }
                }
                appProperties.dropNewDbAfter = Boolean.valueOf(APP_PROPS.getProperty(DROP_NEW_DB_AFTER_PROP, "FALSE"));
                String onlyStructureTables = APP_PROPS.getProperty(ONLY_STRUCTURE_TABLES_PROP, "");
                if(!StringUtils.isNullOrEmpty(onlyStructureTables)) {
                    String[] explodedList = onlyStructureTables.split(",");
                    appProperties.onlyStructureTables = Arrays.stream(explodedList).map(tableName -> tableName.trim()).collect(Collectors.toSet());
                }

                String excludedTables = APP_PROPS.getProperty(EXCLUDED_TABLES_PROP, "");
                if(!StringUtils.isNullOrEmpty(excludedTables)) {
                    String[] explodedList = excludedTables.split(",");
                    appProperties.excludedTables = Arrays.stream(explodedList).map(tableName -> tableName.trim()).collect(Collectors.toSet());
                }

                try {
                    appProperties.batchSize = Integer.valueOf(APP_PROPS.getProperty(BATCH_SIZE_PROP));
                } catch (NumberFormatException e) {
                    LOGGER.debug("Invalid value set for {}, ignoring and using default of {}", BATCH_SIZE_PROP, DEFAULT_BATCH_SIZE);
                    appProperties.batchSize = DEFAULT_BATCH_SIZE;
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

    public String getLocationsIdsString() {
        return locationsIdsString;
    }

    public Set<Integer> getLocationsIds() {
        return locationsIds;
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

    public Integer getBatchSize() {
        return batchSize;
    }

    public LocalDate getEndDate() {
        return endDate;
    }

    public String getFormattedEndDate(String pattern) {
        if(pattern == null) return endDate.format(endDateFormatter);
        else {
            return endDate.format(DateTimeFormatter.ofPattern(pattern));
        }
    }

    public String getLogLevel() {
        return APP_PROPS.getProperty(LOG_LEVEL_PROP, "TRACE").toUpperCase();
    }

    public String getPatientListQueryFileName() {
        return PATIENT_LIST_QUERY_FILE;
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
        String filePath;
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
}
