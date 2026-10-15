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
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import tech.ydb.test.junit5.YdbHelperExtension;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("YDB_HELPER")
public class TestYdbExpressionPushdown
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
    public void testUnicodeProjectionAndDisabledPushdown()
    {
        try (TestTable table = expressionTable()) {
            String sql = "SELECT lower(s), upper(s), strpos(s, '\uD83D\uDE00') FROM " + table.getName();
            String expected = "VALUES (CAST('\u00E9\uD83D\uDE00' AS varchar), CAST('\u00C9\uD83D\uDE00' AS varchar), BIGINT '2'), " +
                    "(CAST('caf\u00E9' AS varchar), CAST('CAF\u00C9' AS varchar), BIGINT '0'), " +
                    "(NULL, NULL, NULL), (CAST('' AS varchar), CAST('' AS varchar), BIGINT '0'), (NULL, NULL, NULL)";
            assertThat(query(sql)).matches(expected).isFullyPushedDown();
            assertThat(query("SELECT s || '!' FROM " + table.getName()))
                    .matches("VALUES CAST('\u00C9\uD83D\uDE00!' AS varchar), CAST('CAF\u00C9!' AS varchar), NULL, CAST('!' AS varchar), NULL")
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT s || repeat('x', " + DEFAULT_MAX_PAGE_SIZE_IN_BYTES + ") FROM " + table.getName() + " WHERE id = 1"))
                    .failure().hasErrorCode(INVALID_FUNCTION_ARGUMENT);
            Session disabledSession = Session.builder(getSession())
                    .setCatalogSessionProperty("local", "complex_expression_pushdown", "false")
                    .build();
            assertThat(query(disabledSession, sql))
                    .matches(expected)
                    .isNotFullyPushedDown(ProjectNode.class);

            JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
            executor.execute("UPSERT INTO " + table.getName() + " (id, n, other, s) VALUES " +
                    "(1, 11, 11, '\u00DF\u0130\u03A3'u), (2, -11, 2, '\u039F\u0394\u039F\u03A3'u)");
            String caseSql = "SELECT lower(s), upper(s) FROM " + table.getName();
            assertThat(query(caseSql)).isFullyPushedDown();
            assertThat(computeActual(caseSql).getMaterializedRows())
                    .containsExactlyInAnyOrderElementsOf(computeActual(disabledSession, caseSql).getMaterializedRows());
        }
    }

    @Test
    public void testColumnComparisonAndNullIf()
    {
        try (TestTable table = expressionTable()) {
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE n = other"))
                    .matches("VALUES BIGINT '1'")
                    .isFullyPushedDown();
            assertThat(query("SELECT nullif(n, BIGINT '11') FROM " + table.getName()))
                    .matches("VALUES CAST(NULL AS bigint), BIGINT '-11', BIGINT '-9223372036854775808', BIGINT '9223372036854775807', CAST(NULL AS bigint)")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testIntegralDivisionAndModulus()
    {
        try (TestTable table = expressionTable()) {
            assertThat(query("SELECT n / 2, n % 2 FROM " + table.getName()))
                    .matches("VALUES (BIGINT '5', BIGINT '1'), (BIGINT '-5', BIGINT '-1'), " +
                            "(BIGINT '-4611686018427387904', BIGINT '0'), (BIGINT '4611686018427387903', BIGINT '1'), " +
                            "(CAST(NULL AS bigint), CAST(NULL AS bigint))")
                    .isFullyPushedDown();
            assertThat(query("SELECT n % BIGINT '-1' FROM " + table.getName()))
                    .matches("VALUES BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', CAST(NULL AS bigint)")
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Test
    public void testArithmeticErrorsRemainTrinoErrors()
    {
        try (TestTable table = expressionTable()) {
            assertThat(query("SELECT n / 0 FROM " + table.getName() + " WHERE id = 1"))
                    .failure().hasErrorCode(DIVISION_BY_ZERO);
            assertThat(query("SELECT n / other FROM " + table.getName() + " WHERE id = 4"))
                    .failure().hasErrorCode(DIVISION_BY_ZERO);
            assertThat(query("SELECT n / BIGINT '-1' FROM " + table.getName() + " WHERE id = 3"))
                    .failure().hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            assertThat(query("SELECT n + BIGINT '1' FROM " + table.getName() + " WHERE id = 4"))
                    .failure().hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    private TestTable expressionTable()
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        TestTable table = new TestTable(executor, "expression_edges_", "(id Int64, n Int64, other Int64, s Utf8, PRIMARY KEY (id))");
        try {
            executor.execute("INSERT INTO " + table.getName() + " (id, n, other, s) VALUES " +
                    "(1, 11, 11, '\u00C9\uD83D\uDE00'u), (2, -11, 2, 'CAF\u00C9'u), " +
                    "(3, CAST('-9223372036854775808' AS Int64), -1, NULL), " +
                    "(4, CAST('9223372036854775807' AS Int64), 0, ''u), (5, NULL, 1, NULL)");
            return table;
        }
        catch (RuntimeException e) {
            table.close();
            throw e;
        }
    }
}
