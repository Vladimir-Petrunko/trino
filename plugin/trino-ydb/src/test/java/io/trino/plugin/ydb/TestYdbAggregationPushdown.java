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
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import tech.ydb.test.junit5.YdbHelperExtension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("YDB_HELPER")
public class TestYdbAggregationPushdown
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
    public void testCountExtremaAndCaseSensitiveDistinct()
    {
        try (TestTable table = aggregationTable()) {
            String sql = "SELECT count(*), count(n), count(DISTINCT n), min(n), max(n), count(DISTINCT s), min(s), max(s), min(u), max(u) FROM " + table.getName();
            assertThat(query(sql))
                    .matches("VALUES (BIGINT '4', BIGINT '3', BIGINT '2', BIGINT '1', BIGINT '2', BIGINT '2', CAST('A' AS varchar), CAST('a' AS varchar), CAST(1 AS decimal(20, 0)), DECIMAL '18446744073709551615')")
                    .isFullyPushedDown();
            assertThat(query(disabledSession(), sql))
                    .matches(getSession(), sql);
            assertQuery(
                    disabledSession(),
                    sql,
                    "VALUES (4, 3, 2, 1, 2, 2, 'A', 'a', CAST(1 AS decimal(20, 0)), CAST(18446744073709551615 AS decimal(20, 0)))",
                    plan -> {
                        assertThat(searchFrom(plan.getRoot()).where(AggregationNode.class::isInstance).findFirst()).isPresent();
                        assertThat(searchFrom(plan.getRoot()).where(TableScanNode.class::isInstance).findFirst()).isPresent();
                    });
            assertThat(query("SELECT s, count(*) FROM " + table.getName() + " GROUP BY s"))
                    .matches("VALUES (CAST('A' AS varchar), BIGINT '2'), (CAST('a' AS varchar), BIGINT '1'), (CAST(NULL AS varchar), BIGINT '1')")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testFloatingSumAndAverage()
    {
        try (TestTable table = aggregationTable()) {
            assertThat(query("SELECT sum(f), avg(f), sum(r), avg(r), count(f) FROM " + table.getName()))
                    .matches("VALUES (DOUBLE '4', DOUBLE '2', REAL '4', REAL '2', BIGINT '2')")
                    .isFullyPushedDown();
            String sql = "SELECT s, sum(f), avg(f) FROM " + table.getName() + " GROUP BY s";
            assertThat(query(sql))
                    .matches(disabledSession(), sql)
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testEmptyInputAndNulls()
    {
        try (TestTable table = aggregationTable()) {
            assertThat(query("SELECT count(*), count(n), min(n), max(n), sum(f), avg(f) FROM " + table.getName() + " WHERE id < 0"))
                    .matches("VALUES (BIGINT '0', BIGINT '0', CAST(NULL AS bigint), CAST(NULL AS bigint), CAST(NULL AS double), CAST(NULL AS double))")
                    .isFullyPushedDown();
            assertThat(query("SELECT count(n), min(n), max(n) FROM " + table.getName() + " WHERE id = 4"))
                    .matches("VALUES (BIGINT '0', CAST(NULL AS bigint), CAST(NULL AS bigint))")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testFloatingPointSpecialValues()
            throws SQLException
    {
        try (TestTable table = aggregationTable();
                Connection connection = DriverManager.getConnection(YdbQueryRunner.buildJdbcUrl(ydb));
                PreparedStatement upsert = connection.prepareStatement("UPSERT INTO " + table.getName() + " (id, f, r) VALUES (?, ?, ?)")) {
            for (double[] values : List.of(
                    new double[] {Double.NaN, 1},
                    new double[] {Double.POSITIVE_INFINITY, 1},
                    new double[] {Double.NEGATIVE_INFINITY, 1},
                    new double[] {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY},
                    new double[] {-0.0, 0.0})) {
                for (int index = 0; index < values.length; index++) {
                    upsert.setLong(1, index + 1);
                    upsert.setDouble(2, values[index]);
                    upsert.setFloat(3, (float) values[index]);
                    upsert.executeUpdate();
                }
                String sql = "SELECT sum(f), avg(f), sum(r), avg(r) FROM " + table.getName();
                assertThat(query(sql))
                        .matches(disabledSession(), sql)
                        .isFullyPushedDown();
            }
        }
    }

    @Test
    public void testUnsupportedAggregationsRemainInTrino()
    {
        try (TestTable table = aggregationTable()) {
            for (String expression : new String[] {"sum(n)", "avg(n)", "min(f)", "max(r)", "count(DISTINCT f)", "sum(DISTINCT f)", "stddev(f)", "variance(f)"}) {
                String sql = "SELECT " + expression + " FROM " + table.getName();
                assertThat(query(sql))
                        .matches(disabledSession(), sql)
                        .isNotFullyPushedDown(AggregationNode.class);
            }
            String sql = "SELECT f, count(*) FROM " + table.getName() + " GROUP BY f";
            assertThat(query(sql))
                    .matches(disabledSession(), sql)
                    .isNotFullyPushedDown(AggregationNode.class);
        }
    }

    @Test
    public void testIntegralSumOverflowRemainsTrinoError()
    {
        try (TestTable table = aggregationTable()) {
            new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb))
                    .execute("UPSERT INTO " + table.getName() + " (id, n) VALUES (1, CAST('9223372036854775807' AS Int64))");
            assertThat(query("SELECT sum(n) FROM " + table.getName()))
                    .failure().hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    private Session disabledSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("local", "aggregation_pushdown_enabled", "false")
                .build();
    }

    private TestTable aggregationTable()
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        TestTable table = new TestTable(executor, "aggregation_edges_", "(id Int64, n Int64, s Utf8, f Double, r Float, u Uint64, PRIMARY KEY (id))");
        try {
            executor.execute("INSERT INTO " + table.getName() + " (id, n, s, f, r, u) VALUES " +
                    "(1, 1, 'A'u, 1.5, 1.5f, 1ul), (2, 1, 'a'u, 2.5, 2.5f, 2ul), " +
                    "(3, 2, 'A'u, NULL, NULL, 18446744073709551615ul), (4, NULL, NULL, NULL, NULL, NULL)");
            return table;
        }
        catch (RuntimeException e) {
            table.close();
            throw e;
        }
    }
}
