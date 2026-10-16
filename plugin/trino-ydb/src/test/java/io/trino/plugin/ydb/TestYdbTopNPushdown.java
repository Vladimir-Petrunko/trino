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
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.plan.TopNNode;
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
import java.sql.Types;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("YDB_HELPER")
public class TestYdbTopNPushdown
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
    public void testFloatingPointOrdering()
            throws SQLException
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = new TestTable(executor, "topn_floating_", "(id Int64, f Double, r Float, PRIMARY KEY (id))");
                Connection connection = DriverManager.getConnection(YdbQueryRunner.buildJdbcUrl(ydb));
                PreparedStatement insert = connection.prepareStatement("INSERT INTO " + table.getName() + " (id, f, r) VALUES (?, ?, ?)")) {
            Double[] values = {Double.NEGATIVE_INFINITY, -1.0, -0.0, 0.0, 1.0, Double.POSITIVE_INFINITY, Double.NaN, null};
            for (int index = 0; index < values.length; index++) {
                insert.setLong(1, index + 1);
                if (values[index] == null) {
                    insert.setNull(2, Types.DOUBLE);
                    insert.setNull(3, Types.REAL);
                }
                else {
                    insert.setDouble(2, values[index]);
                    insert.setFloat(3, values[index].floatValue());
                }
                insert.executeUpdate();
            }
            for (String column : List.of("f", "r")) {
                for (SortOrder order : SortOrder.values()) {
                    String sql = "SELECT id, " + column + " FROM " + table.getName() + " ORDER BY " + column + " " + order + ", id LIMIT 5";
                    assertThat(query(sql)).ordered().matches(disabledSession(), sql).isFullyPushedDown();
                    assertThat(query(disabledSession(), sql)).ordered().matches(getSession(), sql).isNotFullyPushedDown(TopNNode.class);
                }
            }
        }
    }

    @Test
    public void testUnsignedUnicodeAndTemporalOrdering()
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = new TestTable(executor, "topn_types_", "(id Int64, u Uint64, s Utf8, d Date32, ts Timestamp64, PRIMARY KEY (id))")) {
            executor.execute("INSERT INTO " + table.getName() + " (id, u, s, d, ts) VALUES " +
                    "(1, 18446744073709551615ul, '\u00E9'u, Date32('1969-12-31'), Timestamp64('1969-12-31T23:59:59.999999Z')), " +
                    "(2, 0ul, 'A'u, Date32('1970-01-01'), Timestamp64('1970-01-01T00:00:00Z')), " +
                    "(3, 9223372036854775808ul, '\uD83D\uDE00'u, Date32('2000-01-01'), Timestamp64('2000-01-01T00:00:00Z')), " +
                    "(4, NULL, NULL, NULL, NULL)");
            for (String column : List.of("u", "s", "d", "ts")) {
                for (SortOrder order : SortOrder.values()) {
                    String sql = "SELECT id, " + column + " FROM " + table.getName() + " ORDER BY " + column + " " + order + ", id LIMIT 3";
                    assertThat(query(sql)).ordered().matches(disabledSession(), sql).isFullyPushedDown();
                }
            }
        }
    }

    @Test
    public void testNativeDecimalSortRemainsInTrino()
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = new TestTable(executor, "topn_decimal_", "(id Int64, value Decimal(22,9), PRIMARY KEY (id))")) {
            executor.execute("INSERT INTO " + table.getName() + " (id, value) VALUES (1, Decimal('1.25', 22, 9)), (2, Decimal('-1.25', 22, 9)), (3, NULL)");
            String sql = "SELECT id, value FROM " + table.getName() + " ORDER BY value NULLS FIRST, id LIMIT 2";
            assertThat(query(sql)).ordered().matches(disabledSession(), sql).isNotFullyPushedDown(TopNNode.class);
        }
    }

    private Session disabledSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("local", "topn_pushdown_enabled", "false")
                .build();
    }
}
