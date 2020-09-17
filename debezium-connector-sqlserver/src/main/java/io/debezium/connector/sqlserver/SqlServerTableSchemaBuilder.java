package io.debezium.connector.sqlserver;

import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;

public class SqlServerTableSchemaBuilder extends TableSchemaBuilder {

    public SqlServerTableSchemaBuilder(ValueConverterProvider valueConverterProvider,
                                       SchemaNameAdjuster schemaNameAdjuster,
                                       CustomConverterRegistry customConverterRegistry,
                                       Schema sourceInfoSchema, boolean sanitizeFieldNames) {
        super(valueConverterProvider, schemaNameAdjuster, customConverterRegistry, sourceInfoSchema, sanitizeFieldNames);
    }
}
