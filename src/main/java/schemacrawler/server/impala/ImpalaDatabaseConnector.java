package schemacrawler.server.impala;

import java.io.IOException;
import java.util.regex.Pattern;

import schemacrawler.schemacrawler.DatabaseServerType;
import schemacrawler.tools.databaseconnector.DatabaseConnector;
import schemacrawler.tools.iosource.ClasspathInputResource;

public final class ImpalaDatabaseConnector extends DatabaseConnector {
	
	protected ImpalaDatabaseConnector() throws IOException {
		super(new DatabaseServerType("impala", "Apache Impala"),
				new ClasspathInputResource("/help/Connections.impala.txt"),
				new ClasspathInputResource("/schemacrawler-impala.config.properties"), (informationSchemaViewsBuilder,
						connection) -> informationSchemaViewsBuilder.fromResourceFolder("/impala.information_schema"),
				url -> Pattern.matches("jdbc:hive2:.*", url));
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (final ClassNotFoundException e) {
			throw new RuntimeException("Could not load Impala JDBC driver", e);
		}

	}

}
