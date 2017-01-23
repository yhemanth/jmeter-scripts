1. Download https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-2.13.zip and install

2. copy hive-jdbc standalone jar file to $JMETER_HOME/lib/ folder

	e.g: HDP : /usr/hdp/2.5.3.0-37/hive/lib/hive-jdbc-1.2.1000.2.5.3.0-37-standalone.jar
	e.g: For apache master ("mvn clean install -DskipTests -Phadoop-2,dist" and get the hive-jdbc-standalone jar file)

3. Edit $JMETER_HOME/bin/user.properties and add the relevant jar paths.

	e.g For HDP, add the following line in $JMETER_HOME/bin/user.properties.
	
	plugin_dependency_paths=/usr/hdp/2.5.3.0-37/hadoop/lib;/usr/hdp/2.5.3.0-37/hadoop;/usr/hdp/2.5.3.0-37/hive/lib;

	e.g: For Apache master, provide relevant paths in plugin_dependency_paths

4. In case you are using different version of Hive, you may have to copy relevant jars in that folder.

5. run real queries with jmeter with threading. For JDBC URL: In Ambari (HomePage --> Hive --> Summary --> HiveServer2 JDBC URL)

	python jmeter.py -t <threads> -u <jdbc_url> > realX4.jmx

	jmeter -n -t realX4.jmx

	Look for the raw_*.xml and summary_*.xml

