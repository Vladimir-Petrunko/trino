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

import io.trino.Session;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcWriteConfig;
import io.trino.plugin.jdbc.JdbcWriteSessionProperties;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.ydb.test.junit5.YdbHelperExtension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.MERGE_TARGET_ROW_MULTIPLE_MATCHES;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_OPERATION_NUMBER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("YDB_HELPER")
public class TestYdbMerge
        extends AbstractTestQueryFramework
{
    @RegisterExtension
    static final YdbHelperExtension ydb = new YdbHelperExtension().failIfUnavailable();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return YdbQueryRunner.builder(ydb).build();
    }

    @Test
    public void testConditionalMergeWithNullableCompositeKeys()
            throws Exception
    {
        try (TestTable table = new TestTable(
                new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb)),
                "merge_nullable_",
                "(value Int64, tenant Int64, id Int64, flag Bool, label Utf8, PRIMARY KEY (tenant, id))",
                List.of("10, 1, 11, true, 'A'u", "20, NULL, 12, NULL, 'B'u", "30, 2, NULL, false, NULL", "40, NULL, NULL, true, 'C'u"))) {
            try (Connection connection = DriverManager.getConnection(YdbQueryRunner.buildJdbcUrl(ydb));
                    ResultSet rows = connection.getMetaData().getPrimaryKeys(null, null, table.getName())) {
                Map<Short, String> keys = new TreeMap<>();
                while (rows.next()) {
                    keys.put(rows.getShort("KEY_SEQ"), rows.getString("COLUMN_NAME"));
                }
                assertThat(keys.values()).containsExactly("tenant", "id");
            }
            assertUpdate(mergeSession(),
                    """
                    MERGE INTO %s t
                    USING (VALUES
                        (1, 11, 0, 'keep', 'N'),
                        (NULL, 12, 200, 'updated', 'U'),
                        (NULL, NULL, 0, 'deleted', 'D'),
                        (3, 13, 300, 'inserted', 'I')) s(tenant, id, value, label, action)
                    ON t.tenant IS NOT DISTINCT FROM s.tenant AND t.id IS NOT DISTINCT FROM s.id
                    WHEN MATCHED AND s.action = 'D' THEN DELETE
                    WHEN MATCHED AND s.action = 'U' THEN UPDATE SET value = s.value, label = s.label
                    WHEN NOT MATCHED AND s.action = 'I' THEN
                        INSERT (value, tenant, id, flag, label) VALUES (s.value, s.tenant, s.id, false, s.label)
                    """.formatted(table.getName()),
                    3);
            assertQuery("SELECT value, tenant, id, flag, label FROM " + table.getName(),
                    """
                    VALUES (10, 1, 11, true, 'A'), (200, NULL, 12, NULL, 'updated'),
                        (30, 2, NULL, false, NULL), (300, 3, 13, false, 'inserted')
                    """);
        }
    }

    @Test
    public void testSelectiveUpdateCasesAndUnpushedRowChanges()
    {
        try (TestTable table = new TestTable(
                new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb)),
                "merge_cases_",
                "(left_value Int64, right_value Int64, flag Bool, id Int64, PRIMARY KEY (id))",
                List.of("10, 100, true, 1", "20, 200, NULL, 2", "30, 300, false, 3"))) {
            assertUpdate(mergeSession(),
                    """
                    MERGE INTO %s t
                    USING (VALUES (1, 'left', 5), (2, 'right', 7), (3, 'keep', 9)) s(id, action, delta)
                    ON t.id = s.id
                    WHEN MATCHED AND s.action = 'left' THEN UPDATE SET left_value = t.left_value + s.delta
                    WHEN MATCHED AND s.action = 'right' THEN UPDATE SET right_value = NULL, flag = false
                    """.formatted(table.getName()),
                    2);
            assertQuery("SELECT left_value, right_value, flag, id FROM " + table.getName(),
                    "VALUES (15, 100, true, 1), (20, NULL, false, 2), (30, 300, false, 3)");
            assertUpdate(mergeSession(), "UPDATE " + table.getName() + " SET left_value = left_value * 2 WHERE id = 1", 1);
            assertUpdate(mergeSession(), "DELETE FROM " + table.getName() + " WHERE CAST(left_value AS varchar) LIKE '3%'", 2);
            assertQuery("SELECT left_value, right_value, flag, id FROM " + table.getName(), "VALUES (20, NULL, false, 2)");
        }
    }

    @Test
    public void testEmptySourceAndUnmatchedConditions()
    {
        try (TestTable table = new TestTable(
                new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb)),
                "merge_empty_",
                "(value Int64, id Int64, PRIMARY KEY (id))",
                List.of("10, 1"))) {
            assertUpdate(mergeSession(),
                    """
                    MERGE INTO %s t USING (SELECT BIGINT '1' AS id WHERE false) s ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET value = 0
                    WHEN NOT MATCHED THEN INSERT (value, id) VALUES (99, s.id)
                    """.formatted(table.getName()),
                    0);
            assertUpdate(mergeSession(),
                    """
                    MERGE INTO %s t USING (VALUES (1, 99)) s(id, value) ON t.id = s.id
                    WHEN MATCHED AND s.value < 0 THEN UPDATE SET value = s.value
                    """.formatted(table.getName()),
                    0);
            assertQuery("SELECT value, id FROM " + table.getName(), "VALUES (10, 1)");
        }
    }

    @Test
    public void testUnsignedKeyAndNativeTemporalValues()
            throws Exception
    {
        try (TestTable table = new TestTable(
                new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb)),
                "merge_native_",
                "(payload Utf8, u Uint64, d Date32, ts Timestamp64, b String, PRIMARY KEY (u))")) {
            assertUpdate("INSERT INTO " + table.getName() + " (u) VALUES (DECIMAL '18446744073709551615')", 1);
            assertUpdate(mergeSession(),
                    """
                    MERGE INTO %s t
                    USING (VALUES (DECIMAL '18446744073709551615', DATE '1969-12-31',
                        TIMESTAMP '1969-12-31 23:59:59.999999', 'it''s \u00E9\uD83D\uDE00', X'0080FF')) s(u, d, ts, payload, b)
                    ON t.u = s.u
                    WHEN MATCHED THEN UPDATE SET payload = s.payload, d = s.d, ts = s.ts, b = s.b
                    """.formatted(table.getName()),
                    1);
            assertQuery("SELECT CAST(u AS varchar), CAST(d AS varchar), CAST(ts AS varchar), payload, b FROM " + table.getName(),
                    "VALUES ('18446744073709551615', '1969-12-31', '1969-12-31 23:59:59.999999', 'it''s \u00E9\uD83D\uDE00', X'0080FF')");
            try (Connection connection = DriverManager.getConnection(YdbQueryRunner.buildJdbcUrl(ydb));
                    Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery("SELECT CAST(u AS String), CAST(d AS Int32), CAST(ts AS Int64), payload, b FROM " + table.getName())) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("18446744073709551615");
                assertThat(rows.getInt(2)).isEqualTo(-1);
                assertThat(rows.getLong(3)).isEqualTo(-1);
                assertThat(rows.getString(4)).isEqualTo("it's \u00E9\uD83D\uDE00");
                assertThat(rows.getBytes(5)).containsExactly((byte) 0, (byte) 0x80, (byte) 0xFF);
                assertThat(rows.next()).isFalse();
            }
            assertUpdate(mergeSession(),
                    """
                    MERGE INTO %s t USING (VALUES (DECIMAL '18446744073709551615')) s(u) ON t.u = s.u
                    WHEN MATCHED THEN DELETE
                    """.formatted(table.getName()),
                    1);
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 0");
        }
    }

    @Test
    public void testNumericCoercionsWithStablePhysicalKey()
    {
        try (TestTable table = new TestTable(
                new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb)),
                "merge_numeric_",
                "(id Int64, col2 Double, col3 Int32, col4 Int64, col5 Float, col6 Double, PRIMARY KEY (id))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 2, 3, 4, 5, 6)", 1);
            assertUpdate(mergeSession(),
                    """
                    MERGE INTO %s t
                    USING (VALUES (BIGINT '1', REAL '3', DOUBLE '4', 5, 6, REAL '7')) s(id,col2,col3,col4,col5,col6)
                    ON t.id=s.id
                    WHEN MATCHED THEN UPDATE SET col2=s.col2,col3=s.col3,col4=s.col4,col5=s.col5,col6=s.col6
                    """.formatted(table.getName()),
                    1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3.0, 4, 5, 6.0, 7.0)");
        }
    }

    @Test
    public void testPhysicalPrimaryKeyUpdatesAreRejected()
    {
        try (TestTable table = new TestTable(
                new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb)),
                "merge_key_guard_",
                "(value Int64, id Int64, PRIMARY KEY (id))",
                List.of("10, 1"))) {
            assertThat(query(mergeSession(),
                    """
                    MERGE INTO %s t USING (VALUES (1, 2)) s(id, new_id) ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET id = s.new_id
                    """.formatted(table.getName()))).failure().hasErrorCode(NOT_SUPPORTED);
            assertQuery("SELECT value, id FROM " + table.getName(), "VALUES (10, 1)");
        }
    }

    @Test
    public void testDefaultRowChangesAreRejectedWithoutMutation()
    {
        try (TestTable table = new TestTable(
                new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb)),
                "merge_opt_in_",
                "(value Int64, id Int64, PRIMARY KEY (id))",
                List.of("10, 1"))) {
            for (String sql : List.of(
                    "MERGE INTO " + table.getName() + " t USING (VALUES (1, 20)) s(id, value) ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value",
                    "UPDATE " + table.getName() + " SET value = value + 1",
                    "DELETE FROM " + table.getName() + " WHERE CAST(value AS varchar) LIKE '1%'")) {
                assertThat(query(sql)).failure().hasErrorCode(NOT_SUPPORTED);
            }
            assertQuery("SELECT value, id FROM " + table.getName(), "VALUES (10, 1)");
        }
    }

    @Test
    public void testDuplicateSourceTargetMatchIsRejected()
    {
        try (TestTable table = new TestTable(
                new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb)),
                "merge_duplicate_",
                "(value Int64, id Int64, PRIMARY KEY (id))",
                List.of("10, 1"))) {
            assertThat(query(mergeSession(),
                    """
                    MERGE INTO %s t USING (VALUES (1, 20), (1, 30)) s(id, value) ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET value = s.value
                    """.formatted(table.getName()))).failure().hasErrorCode(MERGE_TARGET_ROW_MULTIPLE_MATCHES);
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 1");
        }
    }

    @ParameterizedTest
    @CsvSource({"2,10", "1,99"})
    public void testNativeBatchFailureRespectsTransactionBoundaries(int batchSize, int expectedValue)
    {
        try (TestTable table = new TestTable(
                new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb)),
                "merge_batch_rollback_",
                "(value Int64, id Int64, PRIMARY KEY (id))",
                List.of("10, 1"))) {
            String jdbcUrl = YdbQueryRunner.buildJdbcUrl(ydb) + "&cacheConnectionsInDriver=false&replaceJdbcInByYqlList=false";
            YdbClient client = new YdbClient(
                    new BaseJdbcConfig().setConnectionUrl(jdbcUrl),
                    _ -> DriverManager.getConnection(jdbcUrl),
                    new YdbQueryBuilder(RemoteQueryModifier.NONE),
                    new DefaultIdentifierMapping(),
                    RemoteQueryModifier.NONE);
            ConnectorSession session = TestingConnectorSession.builder()
                    .setPropertyMetadata(new JdbcWriteSessionProperties(new JdbcWriteConfig()).getSessionProperties())
                    .setPropertyValues(Map.of("non_transactional_merge", true))
                    .build();
            JdbcTableHandle target = client.getTableHandle(session, new SchemaTableName("default", table.getName())).orElseThrow();
            JdbcColumnHandle valueColumn = client.getColumns(
                            session,
                            target.getRequiredNamedRelation().getSchemaTableName(),
                            target.getRequiredNamedRelation().getRemoteTableName()).stream()
                    .filter(column -> column.getColumnName().equals("value"))
                    .findFirst().orElseThrow();
            JdbcMergeTableHandle handle = client.beginMerge(
                    session,
                    target,
                    Map.of(0, List.of(valueColumn)),
                    _ -> { throw new AssertionError("Direct batches must own their rollback"); },
                    RetryMode.NO_RETRIES);
            assertThat(handle.getPrimaryKeys()).extracting(JdbcColumnHandle::getColumnName).containsExactly("id");
            List<Block> blocks = new ArrayList<>();
            for (JdbcColumnHandle column : handle.getDataColumns()) {
                blocks.add(switch (column.getColumnName()) {
                    case "value" -> numbers(BIGINT, 99L, 5L);
                    case "id" -> numbers(BIGINT, null, 1L);
                    default -> throw new IllegalArgumentException("Unexpected fixture column: " + column.getColumnName());
                });
            }
            blocks.add(numbers(TINYINT, (long) UPDATE_OPERATION_NUMBER, (long) INSERT_OPERATION_NUMBER));
            blocks.add(numbers(INTEGER, 0L, 0L));
            // Only the UPDATE row carries a row id; the INSERT row has a null row.
            blocks.add(RowBlock.fromNotNullSuppressedFieldBlocks(
                    2, Optional.of(new long[] {1L}), new Block[] {numbers(BIGINT, 1L, null)}));
            Page page = new Page(blocks.toArray(Block[]::new));
            YdbMergeSink sink = new YdbMergeSink(session, handle, client, () -> 1, RemoteQueryModifier.NONE, batchSize);
            try {
                assertTrinoExceptionThrownBy(() -> sink.storeMergedRows(page)).hasErrorCode(JDBC_ERROR);
            }
            finally {
                sink.abort();
            }
            assertQuery("SELECT value, id FROM " + table.getName(), "VALUES (" + expectedValue + ", 1)");
        }
    }

    private Session mergeSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("local", "non_transactional_merge", "true")
                .build();
    }

    private static Block numbers(Type type, Long... values)
    {
        var builder = type.createBlockBuilder(null, values.length);
        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, value);
            }
        }
        return builder.build();
    }
}
