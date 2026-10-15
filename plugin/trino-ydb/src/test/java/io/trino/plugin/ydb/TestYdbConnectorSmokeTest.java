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

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import tech.ydb.test.junit5.YdbHelperExtension;

import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("YDB_HELPER")
public class TestYdbConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @RegisterExtension
    static final YdbHelperExtension ydb = new YdbHelperExtension().failIfUnavailable();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return YdbQueryRunner.builder(ydb)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_TABLE,
                 SUPPORTS_UPDATE,
                 SUPPORTS_DELETE,
                 SUPPORTS_MERGE,
                 SUPPORTS_ADD_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_TRUNCATE,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_DROP_SCHEMA_CASCADE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ARRAY,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_SET_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_DROP_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        Assertions.assertThat(computeScalar("SHOW CREATE TABLE region")).isEqualTo(
                """
                CREATE TABLE %s.%s.region (
                   regionkey bigint,
                   name varchar,
                   comment varchar
                )
                WITH (
                   primary_key = ARRAY['regionkey']
                )""".formatted(catalog, schema));
    }

    // YQL requires a PRIMARY KEY; generic property-free CREATE is not the connector contract.
    // https://ydb.tech/docs/en/yql/reference/syntax/create_table/
    @Test
    @Override
    public void testCreateTable()
    {
        assertThat(query("CREATE TABLE create_without_primary_key (id bigint)"))
                .failure().hasErrorCode(INVALID_TABLE_PROPERTY);
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        assertThat(query("CREATE TABLE ctas_without_primary_key AS SELECT BIGINT '42' id"))
                .failure().hasErrorCode(INVALID_TABLE_PROPERTY);
    }

    @Test
    @Override
    public void testInsert()
    {
        try (TestTable table = newTrinoTable("test_insert_", "(a bigint, b double) WITH (primary_key = ARRAY['a'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) VALUES (42, -38.5), (13, 99.9)", 2);
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + table.getName()))
                    .matches(expectedValues("(42, -38.5), (13, 99.9)"));
        }
    }
}
