/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.ydb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BooleanWriteFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.isNonTransactionalInsert;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.ydb.YdbColumnMappings.YDB_DATE32_SQL_TYPE;
import static io.trino.plugin.ydb.YdbColumnMappings.YDB_TIMESTAMP64_SQL_TYPE;
import static io.trino.plugin.ydb.YdbColumnMappings.dateColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.dateWriteFunction;
import static io.trino.plugin.ydb.YdbColumnMappings.decimalColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.timestampColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.timestampWriteFunction;
import static io.trino.plugin.ydb.YdbColumnMappings.unsignedBigintColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.unsignedColumnMapping;
import static io.trino.plugin.ydb.YdbTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class YdbClient
        extends BaseJdbcClient
{
    static final String DEFAULT_SCHEMA = "default";
    private static final int YDB_DEFAULT_DECIMAL_PRECISION = 22;
    private static final int YDB_DEFAULT_DECIMAL_SCALE = 9;

    @Inject
    public YdbClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("`",
                connectionFactory,
                queryBuilder,
                config.getJdbcTypesMappedToVarchar(),
                identifierMapping,
                remoteQueryModifier,
                false);
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        return ImmutableSet.of(DEFAULT_SCHEMA);
    }

    @Override
    protected String escapeObjectNameForMetadataQuery(String name, String escape)
    {
        return name;
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        // default is a Trino schema; YDB JDBC has no physical schema with that name.
        return super.getTables(
                connection,
                remoteSchemaName.filter(schema -> !DEFAULT_SCHEMA.equalsIgnoreCase(schema)),
                remoteTableName);
    }

    @Override
    protected boolean filterRemoteSchema(String schemaName)
    {
        return DEFAULT_SCHEMA.equalsIgnoreCase(schemaName);
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName table)
    {
        return filterRemoteSchema(table.getSchemaName()) ? super.getTableHandle(session, table) : Optional.empty();
    }

    @Override
    protected String getTableRemoteSchemaName(ResultSet resultSet)
    {
        return DEFAULT_SCHEMA;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(
            ConnectorSession session,
            Connection connection,
            JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        String jdbcTypeName = typeHandle.jdbcTypeName().orElse("").toLowerCase(Locale.ROOT);
        if (jdbcTypeName.equals("bytes") || jdbcTypeName.equals("string")) {
            return Optional.of(varbinaryColumnMapping());
        }
        if (jdbcTypeName.equals("utf8") || jdbcTypeName.equals("text")) {
            return Optional.of(unboundedVarcharColumnMapping());
        }
        Optional<ColumnMapping> primitiveMapping = switch (jdbcTypeName) {
            case "bool" -> Optional.of(booleanColumnMapping());
            case "int8" -> Optional.of(tinyintColumnMapping());
            case "int16" -> Optional.of(smallintColumnMapping());
            case "int32" -> Optional.of(integerColumnMapping());
            case "int64" -> Optional.of(bigintColumnMapping());
            case "uint8" -> Optional.of(unsignedColumnMapping(SMALLINT, 2));
            case "uint16" -> Optional.of(unsignedColumnMapping(INTEGER, 4));
            case "uint32" -> Optional.of(unsignedColumnMapping(BIGINT, 6));
            case "uint64" -> Optional.of(unsignedBigintColumnMapping());
            case "float" -> {
                ColumnMapping valueMapping = realColumnMapping();
                yield Optional.of(ColumnMapping.mapping(REAL, valueMapping.getReadFunction(), valueMapping.getWriteFunction(), DISABLE_PUSHDOWN));
            }
            case "double" -> {
                ColumnMapping valueMapping = doubleColumnMapping();
                yield Optional.of(ColumnMapping.mapping(DOUBLE, valueMapping.getReadFunction(), valueMapping.getWriteFunction(), DISABLE_PUSHDOWN));
            }
            case "date", "date32" -> Optional.of(dateColumnMapping(typeHandle));
            case "datetime", "datetime64", "timestamp", "timestamp64" -> Optional.of(timestampColumnMapping(typeHandle));
            default -> Optional.empty();
        };
        if (primitiveMapping.isPresent()) {
            return primitiveMapping;
        }
        if (typeHandle.jdbcType() != Types.DECIMAL) {
            return getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR ? mapToUnboundedVarchar(typeHandle) : Optional.empty();
        }

        String typeName = typeHandle.jdbcTypeName().orElse("Decimal");
        int precision = typeHandle.columnSize().orElse(YDB_DEFAULT_DECIMAL_PRECISION);
        int scale = typeHandle.decimalDigits().orElse(YDB_DEFAULT_DECIMAL_SCALE);
        int start = typeName.indexOf('(');
        int end = typeName.indexOf(')');
        if (start >= 0 && end > start) {
            String[] parts = typeName.substring(start + 1, end).split(",");
            if (parts.length == 2) {
                Integer typeNamePrecision = Ints.tryParse(parts[0].trim());
                Integer typeNameScale = Ints.tryParse(parts[1].trim());
                if (typeNamePrecision != null && typeNameScale != null) {
                    precision = typeNamePrecision;
                    scale = typeNameScale;
                }
            }
        }

        DecimalType decimalType = createDecimalType(precision, max(scale, 0));
        ColumnMapping decimalMapping = decimalColumnMapping(decimalType);
        return Optional.of(ColumnMapping.mapping(
                decimalType,
                decimalMapping.getReadFunction(),
                decimalMapping.getWriteFunction(),
                DISABLE_PUSHDOWN));
    }

    private static ColumnMapping unboundedVarcharColumnMapping()
    {
        VarcharType varcharType = createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                FULL_PUSHDOWN);
    }

    private static ColumnMapping booleanColumnMapping()
    {
        return ColumnMapping.booleanMapping(
                BOOLEAN,
                ResultSet::getBoolean,
                BooleanWriteFunction.of(Types.BOOLEAN, PreparedStatement::setBoolean));
    }

    @Override
    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        // YDB doesn't use catalog & schema in table names, only the table path
        return quoted(table);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("Bool", BooleanWriteFunction.of(Types.BOOLEAN, PreparedStatement::setBoolean));
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("Int8", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("Int16", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("Int32", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("Int64", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("Float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("Double", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.getPrecision() > 35) {
                throw new TrinoException(NOT_SUPPORTED, "YDB Decimal precision cannot exceed 35");
            }
            String dataType = format("Decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            ColumnMapping mapping = decimalColumnMapping(decimalType);
            return decimalType.isShort()
                    ? WriteMapping.longMapping(dataType, (LongWriteFunction) mapping.getWriteFunction())
                    : WriteMapping.objectMapping(dataType, (ObjectWriteFunction) mapping.getWriteFunction());
        }
        if (type instanceof VarcharType) {
            return WriteMapping.sliceMapping("Text", varcharWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("Bytes", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("Date32", dateWriteFunction(YDB_DATE32_SQL_TYPE));
        }
        if (type == TIMESTAMP_MILLIS || type == TIMESTAMP_MICROS) {
            return WriteMapping.longMapping("Timestamp64", timestampWriteFunction(YDB_TIMESTAMP64_SQL_TYPE));
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping not null constraints");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        // YDB restriction, does not support column comments.
        if (column.getComment().filter(comment -> !comment.isEmpty()).isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
        }

        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType());

        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (column.getDefaultValue().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support defining column defaults through Trino");
        }

        return sb.toString();
    }

    @Override
    public List<JdbcColumnHandle> getPrimaryKeys(ConnectorSession session, RemoteTableName remoteTableName)
    {
        String tableName = remoteTableName.getTableName();
        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_SCHEMA, tableName);
        RemoteTableName metadataTable = new RemoteTableName(Optional.empty(), Optional.empty(), tableName);
        List<JdbcColumnHandle> columns = getColumnsForPrimaryKeyLookup(session, schemaTableName, metadataTable);
        Map<String, JdbcColumnHandle> columnsByName = columns.stream()
                .collect(Collectors.toMap(JdbcColumnHandle::getColumnName, Function.identity()));

        try (Connection connection = getConnection(session)) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet primaryKeyResultSet = metaData.getPrimaryKeys(null, null, tableName)) {
                Map<Short, JdbcColumnHandle> primaryKeysBySequence = new TreeMap<>();

                while (primaryKeyResultSet.next()) {
                    String columnName = primaryKeyResultSet.getString("COLUMN_NAME");
                    JdbcColumnHandle column = columnsByName.get(columnName);
                    if (column == null) {
                        throw new TrinoException(
                                JDBC_ERROR,
                                "Primary key column '%s' is absent from JDBC column metadata for %s"
                                        .formatted(columnName, remoteTableName));
                    }
                    short keySequence = primaryKeyResultSet.getShort("KEY_SEQ");
                    if (primaryKeysBySequence.put(keySequence, column) != null) {
                        throw new TrinoException(
                                JDBC_ERROR,
                                "Duplicate primary key sequence %s for %s".formatted(keySequence, remoteTableName));
                    }
                }
                return ImmutableList.copyOf(primaryKeysBySequence.values());
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to read primary key metadata for " + remoteTableName, e);
        }
    }

    protected List<JdbcColumnHandle> getColumnsForPrimaryKeyLookup(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            RemoteTableName remoteTableName)
    {
        return getColumns(session, schemaTableName, remoteTableName);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        if (isNonTransactionalInsert(session)) {
            Set<String> columnNames = columns.stream()
                    .map(JdbcColumnHandle::getColumnName)
                    .collect(Collectors.toSet());
            if (getPrimaryKeys(session, tableHandle.getRequiredNamedRelation().getRemoteTableName()).stream()
                    .anyMatch(column -> !columnNames.contains(column.getColumnName()))) {
                throw new TrinoException(NOT_SUPPORTED, "Non-transactional YDB inserts require all primary-key columns");
            }
        }
        return super.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        if (isNonTransactionalInsert(session)) {
            super.finishInsertTable(session, handle, pageSinkIds);
            return;
        }

        List<JdbcColumnHandle> missingKeys = getPrimaryKeys(session, handle.getRemoteTableName()).stream()
                .filter(column -> !handle.getColumnNames().contains(column.getColumnName()))
                .toList();
        if (missingKeys.isEmpty()) {
            super.finishInsertTable(session, handle, pageSinkIds);
            return;
        }

        RemoteTableName temporaryTable = new RemoteTableName(
                handle.getRemoteTableName().getCatalogName(),
                handle.getRemoteTableName().getSchemaName(),
                handle.getTemporaryTableName().orElseThrow());
        try (Closer closer = Closer.create()) {
            closer.register(() -> dropTable(session, temporaryTable, true));
            try (Connection connection = getConnection(session, handle)) {
                verify(connection.getAutoCommit());
                List<String> columns = new ArrayList<>(handle.getColumnNames().stream().map(this::quoted).toList());
                List<String> projections = new ArrayList<>(columns);
                for (JdbcColumnHandle key : missingKeys) {
                    columns.add(quoted(key.getColumnName()));
                    projections.add("CAST(NULL AS " + key.getJdbcTypeHandle().jdbcTypeName().orElseThrow() + ") AS " + quoted(key.getColumnName()));
                }
                execute(session, connection, "INSERT INTO %s (%s) SELECT %s FROM %s".formatted(
                        quoted(handle.getRemoteTableName()),
                        String.join(", ", columns),
                        String.join(", ", projections),
                        quoted(temporaryTable)));
            }
        }
        catch (IOException | SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to publish YDB INSERT staging table", e);
        }
    }

    @Override
    protected void copyTableSchema(
            ConnectorSession session,
            Connection connection,
            String catalogName,
            String schemaName,
            String tableName,
            String newTableName,
            List<String> columnNames)
    {
        // YDB JDBC reports neither TABLE_CAT nor TABLE_SCHEM for discovered tables.
        RemoteTableName remoteTable = new RemoteTableName(Optional.empty(), Optional.empty(), tableName);
        Map<String, JdbcColumnHandle> sourceColumns = getColumns(session, new SchemaTableName(DEFAULT_SCHEMA, tableName), remoteTable).stream()
                .collect(Collectors.toMap(JdbcColumnHandle::getColumnName, Function.identity()));
        String stagingKey = "_trino_ydb_staging_key";
        while (columnNames.contains(stagingKey)) {
            stagingKey += "_";
        }
        String declarations = columnNames.stream().map(name -> {
            JdbcColumnHandle column = sourceColumns.get(name);
            String nativeType = column.getJdbcTypeHandle().jdbcTypeName().orElseThrow();
            if (nativeType.equalsIgnoreCase("Decimal")) {
                nativeType = toWriteMapping(session, column.getColumnType()).getDataType();
            }
            return quoted(name) + " " + nativeType + (column.isNullable() ? "" : " NOT NULL");
        }).collect(joining(", "));
        String sql = "CREATE TABLE %s (%s, %s Serial, PRIMARY KEY (%s))".formatted(
                quoted(newTableName), declarations, quoted(stagingKey), quoted(stagingKey));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to create YDB INSERT staging table", e);
        }
    }

    @Override
    public OptionalInt getMaxWriteParallelism(ConnectorSession session)
    {
        return OptionalInt.of(1);
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }

        List<String> primaryKeys = YdbTableProperties.getPrimaryKey(tableMetadata.getProperties());
        if (primaryKeys.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Table property 'primary_key' must contain at least one column");
        }
        if (primaryKeys.contains(null)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Table property 'primary_key' must not contain null columns");
        }
        if (primaryKeys.stream().distinct().count() != primaryKeys.size()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Table property 'primary_key' contains duplicate columns");
        }

        Set<String> columnNames = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toSet());
        primaryKeys.stream()
                .filter(primaryKey -> !columnNames.contains(primaryKey))
                .findFirst()
                .ifPresent(primaryKey -> {
                    throw new TrinoException(
                            INVALID_TABLE_PROPERTY,
                            "Column '%s' specified in table property '%s' does not exist".formatted(primaryKey, PRIMARY_KEY_PROPERTY));
                });

        return List.of("CREATE TABLE %s (%s, PRIMARY KEY (%s))".formatted(
                quoted(remoteTableName),
                String.join(", ", columns),
                primaryKeys.stream().map(this::quoted).collect(joining(", "))));
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        List<String> primaryKeys = getPrimaryKeys(session, tableHandle.getRequiredNamedRelation().getRemoteTableName()).stream()
                .map(JdbcColumnHandle::getColumnName)
                .toList();
        return Map.of(PRIMARY_KEY_PROPERTY, primaryKeys);
    }
}
