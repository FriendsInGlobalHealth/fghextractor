# FGH Extractor Application
Command line application that is used to extract a subset of an OpenMRS database by extracting and copying a set of 
of patients based on specified conditions. These patients' associated data such as names, encounters, obs and others
are all copied. The application also copies all the remaining tables. It does this by creating a new database, copying all
relevant details and then dumping the data into an SQL dump (using mysqldump command)

The application is run on the command line by running an executable jar file generated during the build process.

## Building and Running
In the projects root directory, build the application using maven (Should be version 3.0+) as follows.
```bash
$ mvn clean package
```

The build produces two artifacts in the _target_ directory namely _fgh-extractor-\<version>.jar_ and _fgh-extractor-\<version>-fat-jar.jar_.
The later is executable by _java_ runtime. To run the app you need to provide the _application.properties_ file placing it
in the same directory as the executable jar file. The content of file are as follows.
```properties
# jdbc url is mysql connection string via JDBC
# example -> jdbc.url=jdbc:mysql://localhost:3306/openmrs?autoReconnect=true
jdbc.url=jdbc:mysql://<host>:<port>/<database>?autoReconnect=true
 
# db.username is the mysql user who has read access to database
# replace <username> with an actual user account in the database.
db.username=<username>
 
# db.password is the password the defined user above connects to mysql with.
# Replace <password> with an actual password
db.password=<password>
 
# The new database
# Replace <new database to be created> with an actual value. Usually this can be the location name.
newDb.name=<new database to be created>
 
# Tables you wish to exclude when copying
excluded.tables=
 
# Tables for which you only want to copy structure.
# The list provided below is default. If you don't specify this then the default will take effect.
# If you want to include all tables then leave this blank
copy.only.structure=hl7_in_archive, hl7_in_error, hl7_in_queue, hl7_source
 
# Location ID of the location for which you wish to extract patient data.
# Replace <location ID> with the actual value of the location ID you wish to extract.
location.id=<location ID>
 
# The end date to extract records. If this value is not specified the date of the day when the application
# is run is the default end date. Replace <end date> with an actual date or leave it blank to let default 
# take effect.
end.date=<end date>
 
# The pattern of the provided date in the end.date property, if not provided the assumed default is dd-MM-yyyy
# Replace <end date pattern> with the actual pattern or leave blank to use the default.
end.date.pattern=<end date patter>
 

# Whether to drop the newly created database after backup to sql dump file.
drop.newDb.after=true
    
# The batch sizes to optimize the procedure when the records to be copied are too many. (default is 20000)
batch.size=20000
 
# Application log level. (Currently the application is logging on console, default is trace)
log.level=trace
```

To run the application change into the directory containing the executable jar file then run the as follows, also this is the same location where the
_application.properties_ file is located.
 
**Note** Replace the \<version> with an actual version number for example for 1.0.0 it would be `fghextractor-1.0.0-fat-jar.jar`
```
$ java -jar fghextractor-<version>-fat-jar.jar
```
## Development
The project is setup using maven. During development the developer requires an instance of MySQL with openmrs database.
Also in order to simplify development the developer needs to provide the _dev-application.properties_ in _src/main/resources_ 
directory. The content of this file are the same as the one specified in "Building and Running" section.

To run the application in development mode, simply run the main method of _tz.co.juutech.extractor.FGHExtractorOrchestrator_
class.