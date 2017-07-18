package com.sonyericsson.jenkins.plugins.bfa.db;

import java.util.EnumSet;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.hbm2ddl.SchemaExport.Action;
import org.hibernate.tool.schema.TargetType;

import com.sonyericsson.jenkins.plugins.bfa.model.FailureCause;

/**
 * @author rlamberti
 *
 */
public class SQLSchemaCreator {
	public static void main(String... strings) {
		final SchemaExport export = new SchemaExport();
		export.setDelimiter(";");
		export.setOutputFile("target/ddl_script.sql");
		final StandardServiceRegistry sr = new StandardServiceRegistryBuilder()
				.applySetting("hibernate.dialect", "org.hibernate.dialect.MySQL5Dialect").build();
		final Metadata m = new MetadataSources(sr).addAnnotatedClass(FailureCause.class).buildMetadata();
		export.execute(EnumSet.of(TargetType.SCRIPT, TargetType.STDOUT), Action.CREATE, m);
	}
}
