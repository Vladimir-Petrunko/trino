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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import tech.ydb.test.junit5.YdbHelperExtension;

import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("YDB_HELPER")
public class TestYdbDmlPushdown
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
    public void testUpdateCountsNullableCompositeKeys()
    {
        try (TestTable table = dmlTable()) {
            assertUpdate("UPDATE " + table.getName() + " SET value = 99 WHERE tenant IS NULL", 2);
            assertQuery("SELECT value, tenant, id FROM " + table.getName(),
                    "VALUES (10, 1, 11), (99, NULL, 12), (30, 2, NULL), (99, NULL, NULL)");
            assertUpdate("UPDATE " + table.getName() + " SET value = 10 WHERE id = 11", 1);
            assertUpdate("UPDATE " + table.getName() + " SET value = 77 WHERE id = -1", 0);
        }
    }

    @Test
    public void testMultipleAssignmentsAndNullValues()
    {
        try (TestTable table = dmlTable()) {
            assertUpdate("UPDATE " + table.getName() + " SET value = NULL, flag = false, label = '\u00E9\uD83D\uDE00' WHERE id = 12 AND tenant IS NULL", 1);
            assertQuery("SELECT value, flag, label FROM " + table.getName() + " WHERE id = 12",
                    "VALUES (NULL, false, '\u00E9\uD83D\uDE00')");
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 4");
        }
    }

    @Test
    public void testDeleteCountsNullableCompositeKeys()
    {
        try (TestTable table = dmlTable()) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE tenant IS NULL", 2);
            assertQuery("SELECT value, tenant, id FROM " + table.getName(), "VALUES (10, 1, 11), (30, 2, NULL)");
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = -1", 0);
            assertUpdate("DELETE FROM " + table.getName(), 2);
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 0");
            assertUpdate("UPDATE " + table.getName() + " SET value = 1", 0);
            assertUpdate("DELETE FROM " + table.getName(), 0);
        }
    }

    @Test
    public void testColumnComparisonPredicate()
    {
        try (TestTable table = dmlTable()) {
            new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb))
                    .execute("INSERT INTO " + table.getName() + " (value, tenant, id, flag, label) VALUES (11, 3, 11, false, 'D'u)");
            assertUpdate("UPDATE " + table.getName() + " SET label = 'matched' WHERE value = id", 1);
            assertQuery("SELECT value, tenant, id FROM " + table.getName() + " WHERE label = 'matched'", "VALUES (11, 3, 11)");
            assertUpdate("DELETE FROM " + table.getName() + " WHERE value = id", 1);
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 4");
        }
    }

    @Test
    public void testPhysicalPrimaryKeyUpdatesAreRejected()
    {
        try (TestTable table = dmlTable()) {
            for (String column : List.of("tenant", "id")) {
                assertThat(query("UPDATE " + table.getName() + " SET " + column + " = 3 WHERE value = 10"))
                        .failure().hasErrorCode(NOT_SUPPORTED);
            }
            assertQuery("SELECT value, tenant, id FROM " + table.getName(),
                    "VALUES (10, 1, 11), (20, NULL, 12), (30, 2, NULL), (40, NULL, NULL)");
        }
    }

    @Test
    public void testUnpushedChangesAreRejectedWithoutMutation()
    {
        try (TestTable table = dmlTable()) {
            assertThat(query("UPDATE " + table.getName() + " SET value = value + 1"))
                    .failure().hasErrorCode(NOT_SUPPORTED);
            assertThat(query("DELETE FROM " + table.getName() + " WHERE label LIKE '%A%'"))
                    .failure().hasErrorCode(NOT_SUPPORTED);
            assertQuery("SELECT value, tenant, id FROM " + table.getName(),
                    "VALUES (10, 1, 11), (20, NULL, 12), (30, 2, NULL), (40, NULL, NULL)");
        }
    }

    @Test
    public void testNativeUnsignedTemporalAndQuotedValues()
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = new TestTable(executor, "dml_native_", "(id Int64, u Uint64, d Date32, ts Timestamp64, label Utf8, PRIMARY KEY (id))")) {
            executor.execute("INSERT INTO " + table.getName() + " (id) VALUES (1)");
            assertUpdate("UPDATE " + table.getName() + " SET u = DECIMAL '18446744073709551615', d = DATE '1969-12-31', " +
                    "ts = TIMESTAMP '1969-12-31 23:59:59.999999', label = 'it''s \u00E9\uD83D\uDE00' WHERE id = 1", 1);
            assertQuery("SELECT CAST(u AS varchar), CAST(d AS varchar), CAST(ts AS varchar), label FROM " + table.getName(),
                    "VALUES ('18446744073709551615', '1969-12-31', '1969-12-31 23:59:59.999999', 'it''s \u00E9\uD83D\uDE00')");
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 1", 1);
        }
    }

    private TestTable dmlTable()
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        TestTable table = new TestTable(executor, "dml_composite_", "(value Int64, tenant Int64, id Int64, flag Bool, label Utf8, PRIMARY KEY (tenant, id))");
        try {
            executor.execute("INSERT INTO " + table.getName() + " (value, tenant, id, flag, label) VALUES " +
                    "(10, 1, 11, true, 'A'u), (20, NULL, 12, NULL, 'B'u), (30, 2, NULL, false, NULL), (40, NULL, NULL, true, 'C'u)");
            return table;
        }
        catch (RuntimeException e) {
            try {
                table.close();
            }
            catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }
}
