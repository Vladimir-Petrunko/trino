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

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.trino.plugin.ydb.YdbTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestYdbWriteMetadata
{
    @Test
    public void testExplicitPrimaryKeyWithNamedColumn()
    {
        YdbClient client = new YdbClient(
                new BaseJdbcConfig(),
                _ -> {
                    throw new SQLException("This test must not open a connection");
                },
                new DefaultQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE);
        ConnectorTableMetadata metadata = new ConnectorTableMetadata(
                new SchemaTableName("default", "fixture"),
                List.of(new ColumnMetadata("primary key", BIGINT)),
                Map.of(PRIMARY_KEY_PROPERTY, List.of("primary key")));
        assertThat(client.createTableSqls(
                new RemoteTableName(Optional.empty(), Optional.empty(), "fixture"),
                List.of("`primary key` Int64"),
                metadata))
                .containsExactly("CREATE TABLE `fixture` (`primary key` Int64, PRIMARY KEY (`primary key`))");
    }

    @Test
    public void testStagingUsesDriverTableIdentity()
            throws Exception
    {
        Map<String, Object> row = Map.of(
                "TABLE_NAME", "source",
                "COLUMN_NAME", "payload",
                "DATA_TYPE", Types.BIGINT,
                "TYPE_NAME", "Int64",
                "COLUMN_SIZE", 19,
                "DECIMAL_DIGITS", 0,
                "NULLABLE", DatabaseMetaData.columnNullable);
        AtomicBoolean firstRow = new AtomicBoolean(true);
        ResultSet result = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[] {ResultSet.class},
                (_, method, args) -> switch (method.getName()) {
                    case "next" -> firstRow.getAndSet(false);
                    case "getString", "getInt" -> row.get(args[0]);
                    case "wasNull" -> false;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        AtomicBoolean firstKey = new AtomicBoolean(true);
        ResultSet keys = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[] {ResultSet.class},
                (_, method, _) -> switch (method.getName()) {
                    case "next" -> firstKey.getAndSet(false);
                    case "getString" -> "payload";
                    case "getShort" -> (short) 1;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(DatabaseMetaData.class.getClassLoader(), new Class<?>[] {DatabaseMetaData.class},
                (_, method, args) -> switch (method.getName()) {
                    case "getColumns" -> {
                        assertThat(args[0]).isNull();
                        assertThat(args[1]).isNull();
                        firstRow.set(true);
                        yield result;
                    }
                    case "getPrimaryKeys" -> {
                        assertThat(args[0]).isNull();
                        assertThat(args[1]).isNull();
                        yield keys;
                    }
                    case "getSearchStringEscape" -> "\\";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        List<String> commands = new ArrayList<>();
        Statement statement = (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(), new Class<?>[] {Statement.class},
                (_, method, args) -> {
                    if (method.getName().equals("execute")) {
                        commands.add((String) args[0]);
                        return false;
                    }
                    assertThat(method.getName()).isEqualTo("close");
                    return null;
                });
        Supplier<Connection> connection = () -> (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (_, method, _) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "createStatement" -> statement;
                    case "setReadOnly", "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        YdbClient client = new YdbClient(
                new BaseJdbcConfig(),
                _ -> connection.get(),
                new DefaultQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE);
        assertThat(client.getPrimaryKeys(SESSION, new RemoteTableName(Optional.of("catalog"), Optional.of("default"), "source")))
                .extracting(column -> column.getColumnName())
                .containsExactly("payload");
        try (Connection target = connection.get()) {
            client.copyTableSchema(SESSION, target, null, "default", "source", "staging", List.of("payload"));
        }
        assertThat(commands).containsExactly(
                "CREATE TABLE `staging` (`payload` Int64, `_trino_ydb_staging_key` Serial, PRIMARY KEY (`_trino_ydb_staging_key`))");
    }

    @Test
    public void testRetriesRejectedBeforeOpeningConnection()
    {
        AtomicBoolean opened = new AtomicBoolean();
        YdbClient client = new YdbClient(
                new BaseJdbcConfig(),
                _ -> {
                    opened.set(true);
                    throw new SQLException("This test must not open a connection");
                },
                new DefaultQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE);
        DefaultJdbcMetadata metadata = new DefaultJdbcMetadata(client, TimestampTimeZoneDomain.UTC_ONLY, true, Set.of());
        ConnectorTableMetadata table = new ConnectorTableMetadata(
                new SchemaTableName("default", "fixture"),
                List.of(new ColumnMetadata("id", BIGINT)),
                Map.of(PRIMARY_KEY_PROPERTY, List.of("id")));
        assertThatThrownBy(() -> metadata.beginCreateTable(SESSION, table, Optional.empty(), RetryMode.RETRIES_ENABLED, false))
                .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()));
        assertThat(opened).isFalse();
    }
}
