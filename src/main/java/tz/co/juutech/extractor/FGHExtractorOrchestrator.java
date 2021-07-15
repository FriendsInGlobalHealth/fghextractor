package tz.co.juutech.extractor;

import ch.qos.logback.classic.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tz.co.juutech.extractor.exception.InvalidMandatoryPropertyValueException;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FGHExtractorOrchestrator {
    private static final Logger LOGGER = LoggerFactory.getLogger(FGHExtractorOrchestrator.class);
    static final int NUMBER_OF_THREADS = 5;

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        setLoggingLevel();
        LOGGER.info("START TIME: {}", LocalDateTime.now());
        LOGGER.info("Effective applicatin properties being used are {}", AppProperties.getInstance().toString());

        if(AppProperties.getInstance().getLocationId() == null) {
            LOGGER.error("All mandatory properties must be set with valid values");
            throw new InvalidMandatoryPropertyValueException(AppProperties.LOCATION_ID_PROP,
                    String.valueOf(AppProperties.getInstance().getLocationId()));
        }

        ExecutorService service = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        try {
            // Create the database to copy/extract to.
            LOGGER.debug("Creating the new database {}", AppProperties.getInstance().getNewDatabaseName());
            ExtractionUtils.createNewDatabase(ConnectionPool.getConnection());

            List<String> otherTablesToBeCopied = ExtractionUtils.getListofTablesToMove(ConnectionPool.getConnection());

            // Copy tables which only have to be copied for structure
            LOGGER.debug("Copying tables copied without data: {}", AppProperties.getInstance().getOnlyStructureTables());
            long startOfStep = System.currentTimeMillis();
            ExtractionUtils.copyOnlyStructure(ConnectionPool.getConnection(), AppProperties.getInstance().getOnlyStructureTables());
            LOGGER.debug("Time taken to copy tables for which we only want structure: {} ms", System.currentTimeMillis() - startOfStep);

            // Get the list of patients to copy.
            startOfStep = System.currentTimeMillis();
            String patientListQuery = ExtractionUtils.getPatientListQueryFromFile();
            LOGGER.debug("Patient list query is: {}", patientListQuery);

            StringBuilder patCondition = new StringBuilder("t.patient_id in (").append(patientListQuery).append(")");
            String patSql = ExtractionUtils.getCopyingSQL("patient", patCondition.toString());
            TableCopierTask patientCopier = new TableCopierTask("patient", patCondition.toString());

            StringBuilder personCondition = new StringBuilder("t.person_id in (").append(patientListQuery).append(")");
            String personSql = ExtractionUtils.getCopyingSQL("person", personCondition.toString());
            TableCopierTask personCopier = new TableCopierTask("person", personCondition.toString());

            service.invokeAll(Arrays.asList(patientCopier, personCopier));

            LOGGER.debug("Time taken to copy patient & person table records: {} ms", System.currentTimeMillis() - startOfStep);
            // Tables referencing person.
            startOfStep = System.currentTimeMillis();
            TablesReferencingAnotherTask personTablesTask = new TablesReferencingAnotherTask(ConnectionPool.getConnection(), "person", "person_id");
            TablesReferencingAnotherTask patientTablesTask = new TablesReferencingAnotherTask(ConnectionPool.getConnection(), "patient", "patient_id");

            List<Future<Set<TableReferencingAnother>>> futures = service.invokeAll(Arrays.asList(personTablesTask, patientTablesTask));
            Set<TableReferencingAnother> personReferencingTables = futures.get(0).get();
            Set<TableReferencingAnother> patientReferencingTables = futures.get(1).get();

            List<TableCopierTask> tobeInvoked = new ArrayList<>();
            if (!personReferencingTables.isEmpty()) {
                // Remove from tables to move
                Set<String> personTableNames = personReferencingTables.stream().map(personTable -> personTable.getTable()).collect(Collectors.toSet());
                LOGGER.info("Copying tables having a foreign key referencing the person(person_id) table: {}", personTableNames);
                otherTablesToBeCopied.removeAll(personTableNames);
                for (TableReferencingAnother personRef : personReferencingTables) {
                    StringBuilder tableCondition = new StringBuilder("t.").append(personRef.getColumnName()).append(" in (select person_id from ")
                            .append(AppProperties.getInstance().getNewDatabaseName()).append(".person)");
                    String recordMoverSql = ExtractionUtils.getCopyingSQL(personRef.getTable(), tableCondition.toString());
                    tobeInvoked.add(new TableCopierTask(personRef.getTable(), tableCondition.toString()));
                }
            }

            if (!patientReferencingTables.isEmpty()) {
                // Remove from other tables to move.
                Set<String> tableNames = patientReferencingTables.stream().map(patientTable -> patientTable.getTable()).collect(Collectors.toSet());
                LOGGER.info("Copying tables having a foreign key referencing the patient(patient_id) table: {}", tableNames);
                otherTablesToBeCopied.removeAll(tableNames);
                for (TableReferencingAnother patientRef : patientReferencingTables) {
                    StringBuilder tableCondition = new StringBuilder("t.").append(patientRef.getColumnName()).append(" in (select patient_id from ")
                            .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient)");
                    String recordCopierSql = ExtractionUtils.getCopyingSQL(patientRef.getTable(), tableCondition.toString());
                    tobeInvoked.add(new TableCopierTask(patientRef.getTable(), tableCondition.toString()));
                }
            }

            if(!tobeInvoked.isEmpty()) {
                service.invokeAll(tobeInvoked);
            }
            LOGGER.debug("Time taken to copy table referencing patient & person tables: {} ms", System.currentTimeMillis() - startOfStep);

            // Special handling of encounter_provider & provider tables.
            LOGGER.trace("Copying encounter_provider and provider tables");
            otherTablesToBeCopied.remove("encounter_provider");
            StringBuilder encProvCondition = new StringBuilder("t.encounter_id IN (SELECT encounter_id FROM ")
                    .append(AppProperties.getInstance().getNewDatabaseName()).append(".encounter)");
            String encProvRecordCopierSql = ExtractionUtils.getCopyingSQL("encounter_provider", encProvCondition.toString());
            new TableCopierTask("encounter_provider", encProvCondition.toString()).call();

            LOGGER.debug("Copying providers associated with encounter_provider records who are not yet copied");
            StringBuilder provCondition = new StringBuilder("t.provider_id NOT IN (SELECT provider_id FROM ")
                    .append(AppProperties.getInstance().getNewDatabaseName()).append(".provider)");
            String provCopierSql = ExtractionUtils.getCopyingSQL("provider", provCondition.toString());
            new TableCopierTask("provider", provCondition.toString()).call();
            copyAssociatedPersonAndPatientTablesRecords("provider", personReferencingTables, patientReferencingTables);

            // Copy patient_state records only for copied patients.
            LOGGER.trace("Copying patient_state records for copied patient_program records");
            otherTablesToBeCopied.remove("patient_state");
            StringBuilder patientStateCondition = new StringBuilder("t.patient_program_id IN (SELECT patient_program_id FROM ")
                    .append(AppProperties.getInstance().getNewDatabaseName()).append(".patient_program)");
            String patientStateCopierSql = ExtractionUtils.getCopyingSQL("patient_state", patientStateCondition.toString());
            new TableCopierTask("patient_state", patientStateCondition.toString()).call();

            // Move other tables.
            if (!otherTablesToBeCopied.isEmpty()) {
                LOGGER.info("Copying rest of tables to be copied along with data: {}", otherTablesToBeCopied);
                for (String table : otherTablesToBeCopied) {
                    service.submit(new TableCopierTask(table));
                }
            }

            // Copy users (Find users referenced in every table).
            Set<TableReferencingAnother> tablesReferencingUsers =
                    new TablesReferencingAnotherTask(ConnectionPool.getConnection(), "users", "user_id").call();

            Set<Integer> usersToCopy = new HashSet<>();
            for (TableReferencingAnother table : tablesReferencingUsers) {
                LOGGER.debug("Fetching user ids from {}.{}", table.getTable(), table.getColumnName());
                StringBuilder sb = new StringBuilder("SELECT DISTINCT ").append(table.getColumnName()).append(" FROM ")
                        .append(AppProperties.getInstance().getDatabaseName()).append(".").append(table.getTable())
                        .append(" WHERE ").append(table.getColumnName()).append(" IS NOT NULL");

                try (Connection con = ConnectionPool.getConnection();
                     Statement statement = con.createStatement();
                     ResultSet rs = statement.executeQuery(sb.toString())) {

                    while (rs.next()) {
                        usersToCopy.add(rs.getInt(table.getColumnName()));
                    }
                }
            }

            if (!usersToCopy.isEmpty()) {
                LOGGER.debug("Copying {} users", usersToCopy.size());
                StringBuilder usersCondition = new StringBuilder("t.user_id in (");
                usersToCopy.forEach(id -> usersCondition.append(id).append(","));
                usersCondition.deleteCharAt(usersCondition.length() - 1);
                usersCondition.append(")");
                String userSql = ExtractionUtils.getCopyingSQL("users", usersCondition.toString());
                TableCopierTask usersTask = new TableCopierTask("users", usersCondition.toString());
                usersTask.call();

                copyAssociatedPersonAndPatientTablesRecords("users", personReferencingTables, patientReferencingTables);
            }

            service.shutdown();
            try {
                service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("An error occured while waiting for executor to shutdown", e);
                service.shutdownNow();
            }

            // Extract
            String filename = ExtractionUtils.getDumpFilename();
            StringBuilder cmd = new StringBuilder("mysqldump -u").append(AppProperties.getInstance().getDbUsername())
                    .append(" -p").append(AppProperties.getInstance().getDbPassword())
                    .append(" --host=").append(AppProperties.getInstance().getHost())
                    .append(" --port=").append(AppProperties.getInstance().getPort())
                    .append(" --protocol=").append("tcp")
                    .append(" --compact ").append(AppProperties.getInstance().getNewDatabaseName());

            File file = new File(filename);

            LOGGER.info("Creating SQL dump file {}", filename);

            ProcessBuilder processBuilder = new ProcessBuilder(cmd.toString().split(" "));
            processBuilder.redirectOutput(file);
            Process process = processBuilder.start();
            int exitCode = process.waitFor();

            if(exitCode != 0) {
                BufferedReader buf = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                String line = "";
                LOGGER.error("Error while creating the dump file when running: {}", cmd);
                LOGGER.error("Command exited with exit code {}", exitCode);
                while ((line=buf.readLine())!=null) {
                    LOGGER.error(line);
                }
            } else {
                LOGGER.info("SQL dump file generated successfully");
                BufferedReader buf = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String line = "";
                while ((line=buf.readLine())!=null) {
                    LOGGER.error(line);
                }
            }
        } finally {
            if(AppProperties.getInstance().getDropNewDbAfter()) {
                String sql = "DROP DATABASE IF EXISTS " + AppProperties.getInstance().getNewDatabaseName();
                try(Connection connection = ConnectionPool.getConnection();
                    Statement statement = connection.createStatement()) {
                    LOGGER.debug("Running: {}", sql);
                    statement.execute(sql);
                } catch (Exception e) {
                    // Don't do anything
                }
            }

            if(!service.isTerminated()) {
                service.shutdownNow();
            }

            long timeTaken = System.currentTimeMillis() - start;
            LOGGER.info("FINISH TIME: {}", LocalDateTime.now());
            LOGGER.info("Time taken: {} ms", timeTaken);
        }
    }

    /**
     *
     * @param referencingTable
     * @param idsToCopy String list of integers in the form (6,3,11,56,79)
     * @return
     * @throws SQLException
     */
    private static TableCopierTask copyReferencingTableRecordsTask(final TableReferencingAnother referencingTable, final String idsToCopy) throws SQLException {
        StringBuilder tableCondition = new StringBuilder("t.").append(referencingTable.getColumnName()).append(" in ").append(idsToCopy);
        String recordCopierSql = ExtractionUtils.getCopyingSQL(referencingTable.getTable(), tableCondition.toString());
        return new TableCopierTask(referencingTable.getTable(), tableCondition.toString());
    }

    private static void setLoggingLevel() {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.toLevel(AppProperties.getInstance().getLogLevel(), Level.TRACE));
    }

    private static void copyAssociatedPersonAndPatientTablesRecords(String table, Set<TableReferencingAnother> personReferencingTables,
                                                                    Set<TableReferencingAnother> patientReferencingTables) throws SQLException {
        LOGGER.debug("Fetching person ids for copied {} records whose corresponding person records are not yet copied", table);
        Set<Integer> associatedPersonIds = new HashSet<>();
        StringBuilder sb = new StringBuilder("SELECT person_id FROM ").append(AppProperties.getInstance().getNewDatabaseName())
                .append(".").append(table).append(" u WHERE NOT EXISTS (SELECT 1 FROM ").append(AppProperties.getInstance().getNewDatabaseName())
                .append(".person p WHERE u.person_id = p.person_id)");
        LOGGER.trace("Query fetching person records referenced by not yet copied: {} ", sb.toString());

        try (Connection con = ConnectionPool.getConnection();
             Statement statement = con.createStatement();
             ResultSet rs = statement.executeQuery(sb.toString())) {
            while (rs.next()) {
                associatedPersonIds.add(rs.getInt("person_id"));
            }
        }

        if (!associatedPersonIds.isEmpty()) {
            String setOfIds = ExtractionUtils.stringifySetOfIntegers(associatedPersonIds);
            LOGGER.trace("set of person_id found for table {} is {}", table, setOfIds);
            StringBuilder personTableCondition = new StringBuilder("t.person_id in ").append(setOfIds);
            new TableCopierTask("person", personTableCondition.toString()).call();

            if (!personReferencingTables.isEmpty()) {
                for (TableReferencingAnother personRef : personReferencingTables) {
                    if(table.equals(personRef.getTable())) continue;
                    copyReferencingTableRecordsTask(personRef, setOfIds).call();
                }
            }

            // Do the same for patient tables
            StringBuilder patientTableCondition = new StringBuilder("t.patient_id in ").append(setOfIds);
            new TableCopierTask("patient", patientTableCondition.toString()).call();

            if (!patientReferencingTables.isEmpty()) {
                for (TableReferencingAnother patientRef : patientReferencingTables) {
                    if(table.equals(patientRef.getTable())) continue;
                    copyReferencingTableRecordsTask(patientRef, setOfIds).call();
                }
            }
        }
    }
}
