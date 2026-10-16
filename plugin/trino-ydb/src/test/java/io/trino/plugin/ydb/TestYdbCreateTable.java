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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.trino.Session;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ResourceLock;
import tech.ydb.test.junit5.YdbHelperExtension;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@ResourceLock("YDB_HELPER")
@Execution(SAME_THREAD)
public class TestYdbCreateTable
        extends AbstractTestQueryFramework
{
    @RegisterExtension
    static final YdbHelperExtension ydb = new YdbHelperExtension().failIfUnavailable();
    private final ConcurrentMap<String, CompletableFuture<Void>> rollbacks = new ConcurrentHashMap<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return YdbQueryRunner.builder(ydb)
                .addConnectorProperty("insert.non-transactional-insert.enabled", "false")
                .addConnectorProperty("merge.non-transactional-merge.enabled", "false")
                .setClientModule(Modules.override(new YdbClientModule()).with(new AbstractModule()
                {
                    @Provides
                    @Singleton
                    @ForBaseJdbc
                    public JdbcClient client(
                            BaseJdbcConfig config,
                            ConnectionFactory connectionFactory,
                            QueryBuilder queryBuilder,
                            IdentifierMapping identifierMapping,
                            RemoteQueryModifier modifier)
                    {
                        return new YdbClient(config, connectionFactory, queryBuilder, identifierMapping, modifier)
                        {
                            @Override
                            public void rollbackTemporaryTableCreation(ConnectorSession session, JdbcOutputTableHandle handle)
                            {
                                CompletableFuture<Void> completion = rollbacks.get(handle.getRemoteTableName().getTableName());
                                try {
                                    super.rollbackTemporaryTableCreation(session, handle);
                                    if (completion != null) {
                                        completion.complete(null);
                                    }
                                }
                                catch (RuntimeException e) {
                                    if (completion != null) {
                                        completion.completeExceptionally(e);
                                    }
                                    throw e;
                                }
                            }
                        };
                    }
                }))
                .build();
    }

    @Test
    public void testCreateTableWithPrimaryKey()
    {
        String tableName = "production_create_with_pk";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (tenant bigint, event_id bigint, payload varchar) " +
                    "WITH (primary_key = ARRAY['event_id', 'tenant'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 10, 'a'), (2, NULL, 'b')", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 10, 'a'), (2, NULL, 'b')");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                    .contains("primary_key = ARRAY['event_id','tenant']");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateTableAsSelectWithPrimaryKey()
            throws Exception
    {
        String tableName = "production_ctas_with_pk";
        String duplicateTableName = "production_ctas_duplicate_pk";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id, payload) WITH (primary_key = ARRAY['id']) " +
                    "AS VALUES (BIGINT '1', 'a'), (BIGINT '2', 'b')", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (CAST(1 AS BIGINT), 'a'), (CAST(2 AS BIGINT), 'b')");

            var tablesBefore = computeActual("SHOW TABLES").getOnlyColumnAsSet();
            CompletableFuture<Void> rollback = new CompletableFuture<>();
            rollbacks.put(duplicateTableName, rollback);
            assertThat(query("CREATE TABLE " + duplicateTableName + " (id, payload) " +
                    "WITH (primary_key = ARRAY['id']) AS VALUES (BIGINT '1', 'a'), (BIGINT '1', 'b')"))
                    .failure().hasErrorCode(JDBC_ERROR);
            rollback.get(30, SECONDS);
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).isEqualTo(tablesBefore);
            assertThat(getQueryRunner().tableExists(getSession(), duplicateTableName)).isFalse();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            assertUpdate("DROP TABLE IF EXISTS " + duplicateTableName);
            rollbacks.remove(duplicateTableName);
        }
    }

    @Test
    public void testTransactionalInsertAndPrimaryKeyUpdateRejection()
    {
        String table = "production_transactional_insert";
        try {
            assertUpdate("CREATE TABLE " + table + " (payload varchar, id bigint) WITH (primary_key = ARRAY['id'])");
            assertUpdate("INSERT INTO " + table + " VALUES ('a', 1), ('b', 2)", 2);
            var tablesBefore = computeActual("SHOW TABLES").getOnlyColumnAsSet();
            assertThat(query("INSERT INTO " + table + " VALUES ('c', 3), ('duplicate', 1)")).failure().hasErrorCode(JDBC_ERROR);
            assertQuery("SELECT * FROM " + table, "VALUES ('a', CAST(1 AS BIGINT)), ('b', CAST(2 AS BIGINT))");
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).isEqualTo(tablesBefore);
            assertThat(query("UPDATE " + table + " SET id = 3 WHERE id = 1")).failure().hasErrorCode(NOT_SUPPORTED);
            assertThat(query("UPDATE " + table + " SET payload = upper(payload) WHERE id = 2")).failure().hasErrorCode(NOT_SUPPORTED);
            assertThat(query("DELETE FROM " + table + " WHERE payload LIKE 'a%'")).failure().hasErrorCode(NOT_SUPPORTED);
            assertQuery("SELECT * FROM " + table, "VALUES ('a', CAST(1 AS BIGINT)), ('b', CAST(2 AS BIGINT))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testNonTransactionalInsertRequiresExplicitKeys()
    {
        Session nonTransactional = Session.builder(getSession())
                .setCatalogSessionProperty("local", "non_transactional_insert", "true")
                .build();
        try (TestTable table = newTrinoTable(
                "non_transactional_keys_",
                "(payload varchar, id bigint, tenant bigint) WITH (primary_key = ARRAY['id', 'tenant'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " (payload, id) VALUES ('staged', 1)", 1);
            assertUpdate(nonTransactional, "INSERT INTO " + table.getName() + " VALUES ('direct', 2, 2)", 1);
            assertThat(query(nonTransactional, "INSERT INTO " + table.getName() + " (payload, id) VALUES ('rejected', 3)"))
                    .failure().hasErrorCode(NOT_SUPPORTED);
            assertQuery("SELECT * FROM " + table.getName(),
                    "VALUES ('staged', CAST(1 AS BIGINT), CAST(NULL AS BIGINT)), ('direct', CAST(2 AS BIGINT), CAST(2 AS BIGINT))");
        }
    }

    @Test
    public void testInvalidPrimaryKeyProperties()
    {
        String tableName = "production_create_invalid_pk";
        assertQueryFails("CREATE TABLE " + tableName + " (id bigint)",
                ".*Table property 'primary_key' must contain at least one column");
        assertQueryFails(
                "CREATE TABLE " + tableName + " (id bigint) " +
                        "WITH (primary_key = CAST(ARRAY[] AS ARRAY(VARCHAR)))",
                ".*Table property 'primary_key' must contain at least one column");
        assertQueryFails("CREATE TABLE " + tableName + " (id bigint) WITH (primary_key = ARRAY['missing'])",
                ".*Column 'missing' specified in table property 'primary_key' does not exist");
        assertQueryFails("CREATE TABLE " + tableName + " (id bigint) WITH (primary_key = ARRAY['id', 'id'])",
                ".*Table property 'primary_key' contains duplicate columns");
        assertQueryFails("CREATE TABLE " + tableName + " (id bigint) WITH (primary_key = ARRAY[CAST(NULL AS VARCHAR)])",
                ".*Table property 'primary_key' must not contain null columns");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testScalarWriteTypes()
    {
        String table = "production_scalar_writes";
        try {
            assertUpdate("CREATE TABLE " + table + " (id bigint, bool_value boolean, tiny_value tinyint, small_value smallint, " +
                    "int_value integer, bigint_value bigint, real_value real, double_value double, decimal_value decimal(22,9), " +
                    "text_value varchar, binary_value varbinary, date_value date, timestamp_value timestamp(6)) " +
                    "WITH (primary_key = ARRAY['id'])");
            assertUpdate("INSERT INTO " + table + " VALUES (1, true, TINYINT '-128', SMALLINT '32767', INTEGER '-2147483648', " +
                    "BIGINT '-9223372036854775808', REAL '1.25', DOUBLE '-2.5', DECIMAL '1.250000000', 'hello', X'0080FF', " +
                    "DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999999'), " +
                    "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", 2);
            assertQuery("SELECT * FROM " + table,
                    "VALUES (CAST(1 AS BIGINT), true, CAST(-128 AS TINYINT), CAST(32767 AS SMALLINT), -2147483648, " +
                            "CAST('-9223372036854775808' AS BIGINT), CAST(1.25 AS REAL), CAST(-2.5 AS DOUBLE), " +
                            "CAST('1.25' AS DECIMAL(22,9)), 'hello', X'0080FF', DATE '1969-12-31', " +
                            "TIMESTAMP '1969-12-31 23:59:59.999999'), (CAST(2 AS BIGINT), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testInsertIntoNativeUnsignedAndTemporalColumns()
            throws Exception
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        TestTable table = new TestTable(executor, "native_writes_", "(id Int64, u Uint64, d Date32, t Timestamp64, PRIMARY KEY (id))");
        try (table) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, DECIMAL '18446744073709551615', " +
                    "DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999999')", 1);
            for (String value : List.of("-1", "18446744073709551616")) {
                CompletableFuture<Void> rollback = new CompletableFuture<>();
                rollbacks.put(table.getName(), rollback);
                assertThat(query("INSERT INTO " + table.getName() + " (id, u) VALUES (2, DECIMAL '" + value + "')"))
                        .failure().hasErrorCode(JDBC_ERROR);
                rollback.get(30, SECONDS);
            }
            assertQuery("SELECT * FROM " + table.getName(),
                    "VALUES (CAST(1 AS BIGINT), CAST('18446744073709551615' AS DECIMAL(20,0)), " +
                            "DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999999')");
        }
        finally {
            rollbacks.remove(table.getName());
        }
    }
}
