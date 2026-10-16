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
import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import tech.ydb.test.junit5.YdbHelperExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.ydb.YdbTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ResourceLock("YDB_HELPER")
public class TestYdbConnectorTest
        extends BaseConnectorTest
{
    @RegisterExtension
    static final YdbHelperExtension ydb = new YdbHelperExtension().failIfUnavailable();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return YdbQueryRunner.builder(ydb)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .amendSession(session -> session.setCatalogSessionProperty("local", "non_transactional_merge", "true"))
                .build();
    }

    @Override
    protected TestTable createTestTableForWrites(String namePrefix, String tableDefinition, String primaryKey)
    {
        return createTestTableForWrites(namePrefix, tableDefinition, List.of(), primaryKey);
    }

    @Override
    protected TestTable createTestTableForWrites(String namePrefix, String tableDefinition, List<String> rowsToInsert, String primaryKey)
    {
        SqlExecutor executor = sql -> {
            Statement statement = new SqlParser().createStatement(sql);
            getQueryRunner().execute(statement instanceof CreateTable || statement instanceof CreateTableAsSelect
                    ? withPrimaryKey(statement, Optional.of(primaryKey))
                    : sql);
        };
        return new TestTable(executor, namePrefix, tableDefinition, rowsToInsert);
    }

    @Override
    protected void createTableForWrites(String createTable, String tableName, Optional<String> primaryKey, OptionalInt updateCount)
    {
        String sql = withPrimaryKey(new SqlParser().createStatement(createTable.formatted(tableName)), primaryKey);
        updateCount.ifPresentOrElse(count -> assertUpdate(sql, count), () -> assertUpdate(sql));
    }

    private static String withPrimaryKey(Statement statement, Optional<String> primaryKey)
    {
        // Source fixtures nominate no key. Their non-floating columns are unique together;
        // declare that real composite key without adding or hiding any fixture columns.
        List<String> keys = primaryKey
                .map(key -> Arrays.stream(key.split(",")).map(String::trim).toList())
                .orElseGet(() -> ((CreateTable) statement).getElements().stream()
                        .map(ColumnDefinition.class::cast)
                        .filter(column -> !(column.getType() instanceof GenericDataType type) ||
                                !Set.of("DOUBLE", "REAL").contains(type.getName().getValue().toUpperCase(ENGLISH)))
                        .map(column -> column.getName().getSuffix())
                        .toList());
        Property property = new Property(
                new Identifier(PRIMARY_KEY_PROPERTY),
                new Array(keys.stream().map(key -> (Expression) new StringLiteral(key)).toList()));
        Statement keyed = switch (statement) {
            case CreateTable table -> new CreateTable(
                    table.getLocation().orElseThrow(),
                    table.getName(),
                    table.getElements(),
                    table.getSaveMode(),
                    ImmutableList.<Property>builder().addAll(table.getProperties()).add(property).build(),
                    table.getComment());
            case CreateTableAsSelect table -> new CreateTableAsSelect(
                    table.getLocation().orElseThrow(),
                    table.getName(),
                    table.getQuery(),
                    table.getSaveMode(),
                    ImmutableList.<Property>builder().addAll(table.getProperties()).add(property).build(),
                    table.isWithData(),
                    table.getColumnAliases(),
                    table.getComment());
            default -> throw new IllegalArgumentException("Expected CREATE TABLE fixture: " + statement.getClass().getSimpleName());
        };
        return formatSql(keyed);
    }

    @Test
    @Override
    public void testUpdate()
    {
        try (TestTable table = createTestTableForWrites("test_row_update_", "AS SELECT * FROM nation", "name")) {
            assertUpdate("UPDATE " + table.getName() + " SET nationkey = 100 WHERE regionkey = 2", 5);
            assertQuery("SELECT count(*) FROM " + table.getName() + " WHERE nationkey = 100", "VALUES 5");
        }
    }

    @Test
    @Override
    public void testUpdateMultipleCondition()
    {
        try (TestTable table = createTestTableForWrites(
                "test_row_update_", "(id INTEGER, a INTEGER, b INTEGER)", List.of("1, 1, 10", "2, 1, 20", "3, 2, 10"), "id")) {
            assertUpdate("UPDATE " + table.getName() + " SET b = 100 WHERE a = 1 AND b = 10", 1);
            assertQuery("SELECT id, a, b FROM " + table.getName(), "VALUES (1, 1, 100), (2, 1, 20), (3, 2, 10)");
        }
    }

    @Test
    @Override
    public void testUpdateWithNullValues()
    {
        try (TestTable table = createTestTableForWrites("test_update_nulls_", "AS SELECT * FROM nation", "name")) {
            assertQuery("SELECT count(*) FROM " + table.getName() + " WHERE nationkey IS NULL", "VALUES 0");
            assertUpdate("UPDATE " + table.getName() + " SET nationkey = NULL WHERE regionkey = 2", 5);
            assertQuery("SELECT count(*) FROM " + table.getName() + " WHERE nationkey IS NULL", "VALUES 5");
        }
        try (TestTable table = createTestTableForWrites(
                "test_update_nulls_", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL, pk INT)", "pk")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 1, 1), (2, 2, 2)", 2);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 1, 1), (2, 2, 2)");
            assertUpdate("UPDATE " + table.getName() + " SET nullable_col = NULL WHERE not_null_col = 1", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 1, 1), (2, 2, 2)");
        }
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        try (TestTable table = createTestTableForWrites("test_update_", "AS TABLE tpch.tiny.nation", "name")) {
            assertUpdate("UPDATE " + table.getName() + " SET nationkey = 100 + nationkey WHERE regionkey = 2", 5);
            assertThat(query("SELECT CAST(nationkey AS bigint), name, CAST(regionkey AS bigint), comment FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT IF(regionkey=2, nationkey + 100, nationkey) nationkey, name, regionkey, comment FROM tpch.tiny.nation");
            assertUpdate("UPDATE " + table.getName() + " SET nationkey = nationkey * 2 WHERE regionkey IN (2,3)", 10);
            assertThat(query("SELECT CAST(nationkey AS bigint), name, CAST(regionkey AS bigint), comment FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT CASE regionkey WHEN 2 THEN 2*(nationkey+100) WHEN 3 THEN 2*nationkey ELSE nationkey END nationkey, name, regionkey, comment FROM tpch.tiny.nation");
        }
    }

    @Test
    @Override
    public void testUpdateWithPredicates()
    {
        try (TestTable table = createTestTableForWrites(
                "test_update_predicates_", "(id INT, a INT, b INT, c INT)", List.of("1, 1, 2, 3", "2, 11, 12, 13", "3, 21, 22, 23"), "id")) {
            assertUpdate("UPDATE " + table.getName() + " SET a = a - 1 WHERE c = 3", 1);
            assertQuery("SELECT id, a, b, c FROM " + table.getName(), "VALUES (1, 0, 2, 3), (2, 11, 12, 13), (3, 21, 22, 23)");
            assertUpdate("UPDATE " + table.getName() + " SET c = c + 1 WHERE a = 11", 1);
            assertQuery("SELECT id, a, b, c FROM " + table.getName(), "VALUES (1, 0, 2, 3), (2, 11, 12, 14), (3, 21, 22, 23)");
            assertUpdate("UPDATE " + table.getName() + " SET b = b * 2 WHERE b = 22", 1);
            assertQuery("SELECT id, a, b, c FROM " + table.getName(), "VALUES (1, 0, 2, 3), (2, 11, 12, 14), (3, 21, 44, 23)");
        }
    }

    @Test
    @Override
    public void testUpdateAllValues()
    {
        try (TestTable table = createTestTableForWrites(
                "test_update_all_values_", "(id INT, a INT, b INT, c INT)", List.of("1, 1, 2, 3", "2, 11, 12, 13", "3, 21, 22, 23"), "id")) {
            assertUpdate("UPDATE " + table.getName() + " SET a = a + 1, b = b - 1, c = c * 2", 3);
            assertQuery("SELECT id, a, b, c FROM " + table.getName(), "VALUES (1, 2, 1, 6), (2, 12, 11, 26), (3, 22, 21, 46)");
        }
    }

    @Test
    @Override
    public void testUpdateCaseSensitivity()
    {
        try (TestTable table = createTestTableForWrites("test_row_update_", "AS SELECT * FROM nation", "name")) {
            assertUpdate("UPDATE " + table.getName() + " SET NATIONKEY = 100 WHERE REGIONKEY = 2", 5);
            assertQuery("SELECT count(*) FROM " + table.getName() + " WHERE nationkey = 100", "VALUES 5");
        }
    }

    @Test
    @Override
    public void testMergeFruits()
    {
        try (TestTable target = createTestTableForWrites(
                "merge_fruits_target_",
                "(customer VARCHAR, purchase VARCHAR)",
                List.of("'Dave', 'dates'", "'Lou', 'limes'", "'Carol', 'candles'"),
                "purchase");
                TestTable source = createTestTableForWrites(
                        "merge_fruits_source_",
                        "(customer VARCHAR, purchase VARCHAR)",
                        List.of("'Craig', 'candles'", "'Len', 'limes'", "'Joe', 'jellybeans'"),
                        "customer,purchase")) {
            assertUpdate("MERGE INTO " + target.getName() + " t USING " + source.getName() + " s ON t.purchase = s.purchase " +
                    "WHEN MATCHED AND s.purchase = 'limes' THEN DELETE " +
                    "WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer) " +
                    "WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES (s.customer, s.purchase)", 3);
            assertQuery("SELECT * FROM " + target.getName(),
                    "VALUES ('Dave', 'dates'), ('Carol_Craig', 'candles'), ('Joe', 'jellybeans')");
        }
    }

    // Every column includes the physical key, which YQL UPDATE cannot change.
    // https://ydb.tech/docs/en/yql/reference/syntax/update
    @Test
    @Override
    public void testMergeAllColumnsUpdated()
    {
        try (TestTable table = createTestTableForWrites(
                "merge_all_columns_", "(customer VARCHAR, purchases INT, address VARCHAR)", List.of("'Aaron', 5, 'Antioch'"), "customer")) {
            assertThat(query("MERGE INTO " + table.getName() + " t USING (VALUES ('Aaron', 6, 'Arches')) s(customer,purchases,address) " +
                    "ON t.customer=s.customer WHEN MATCHED THEN UPDATE SET customer=CONCAT(t.customer,'_updated'),purchases=s.purchases+t.purchases,address=s.address"))
                    .failure().hasErrorCode(NOT_SUPPORTED);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES ('Aaron', 5, 'Antioch')");
        }
    }

    // The inherited statement also updates physical key col1; non-key casts are tested separately.
    // https://ydb.tech/docs/en/yql/reference/syntax/update
    @Test
    @Override
    public void testMergeCasts()
    {
        try (TestTable table = createTestTableForWrites(
                "merge_cast_key_",
                "(col1 INT, col2 DOUBLE, col3 INT, col4 BIGINT, col5 REAL, col6 DOUBLE)",
                List.of("1, 2, 3, 4, 5, 6"),
                "col1")) {
            assertThat(query("MERGE INTO " + table.getName() + " t " +
                    "USING (VALUES (BIGINT '2', REAL '3', DOUBLE '4', 5, 6, REAL '7')) s(col1,col2,col3,col4,col5,col6) " +
                    "ON t.col1+1=s.col1 WHEN MATCHED THEN UPDATE SET col1=s.col1,col2=s.col2,col3=s.col3,col4=s.col4,col5=s.col5,col6=s.col6"))
                    .failure().hasErrorCode(NOT_SUPPORTED);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 2.0, 3, 4, 5.0, 6.0)");
        }
    }

    // Reversing assignment order does not permit updating physical key a.
    // https://ydb.tech/docs/en/yql/reference/syntax/update
    @Test
    @Override
    public void testMergeAllColumnsReversed()
    {
        try (TestTable table = createTestTableForWrites("merge_reversed_key_", "(a INT, b INT, c INT)", List.of("1, 2, 3"), "a")) {
            assertThat(query("MERGE INTO " + table.getName() + " t USING (VALUES 1) s(a) ON t.a=s.a " +
                    "WHEN MATCHED THEN UPDATE SET c=100,b=42,a=0"))
                    .failure().hasErrorCode(NOT_SUPPORTED);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 2, 3)");
        }
    }

    @Test
    public void testDefaultSchema()
    {
        assertThat(computeActual("SHOW CATALOGS").getOnlyColumnAsSet()).contains("local");
        assertThat(computeActual("SHOW SCHEMAS FROM local").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("default", "information_schema");
        assertThat(computeActual("SHOW TABLES FROM local.default").getOnlyColumnAsSet()).contains("orders");
        assertQuerySucceeds("SELECT orderkey FROM local.default.orders LIMIT 1");
        assertQueryFails("SELECT * FROM local.missing.orders", ".*Schema 'missing' does not exist");
        assertQueryFails("SELECT * FROM local.\"%\".orders", ".*Schema '%' does not exist");
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_TABLE,
                 SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
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
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_UPDATE,
                 SUPPORTS_DELETE,
                 SUPPORTS_MERGE,
                 SUPPORTS_ROW_LEVEL_UPDATE,
                 SUPPORTS_ROW_LEVEL_DELETE,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected String tableDefinitionForAddColumn()
    {
        return "(x varchar) WITH (primary_key = ARRAY['x'])";
    }

    @Override
    protected String createTableSqlForAddingAndDroppingColumn(String tableName, String columnNameInSql)
    {
        return super.createTableSqlForAddingAndDroppingColumn(tableName, columnNameInSql) + " WITH (primary_key = ARRAY['value'])";
    }

    @Test
    @Override
    public void testAlterTableAddLongColumnName()
    {
        String tableName = "test_long_column" + randomNameSuffix();
        String createTable = "CREATE TABLE " + tableName + " WITH (primary_key = ARRAY['x']) AS SELECT 123 x";
        String validColumnName = "col" + "z".repeat(maxColumnNameLength().orElseThrow() - 3);
        assertUpdate(createTable, 1);
        try {
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + validColumnName + " int");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
            assertQuery("SELECT x FROM " + tableName, "VALUES 123");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }

        assertUpdate(createTable, 1);
        try {
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + validColumnName + "z int"))
                    .satisfies(this::verifyColumnNameLengthFailurePermissible);
            assertQuery("SELECT x FROM " + tableName, "VALUES 123");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception exception)
    {
        // Overlapping YDB scheme operations on one table may report OVERLOADED.
        // https://ydb.tech/docs/en/reference/ydb-sdk/ydb-status-codes
        assertThat(exception)
                .hasMessageContaining("Status{code = OVERLOADED(code=400060)")
                .hasMessageContaining("error: path is under operation")
                .hasMessageContaining("state: EPathStateAlter");
    }

    // Generic DDL fixtures omit YDB's mandatory primary key.
    // https://ydb.tech/docs/en/yql/reference/syntax/create_table/
    @Test
    @Override
    public void testAddNotNullColumnToEmptyTable()
    {
        try (TestTable table = newTrinoTable("test_add_nn_to_empty", "(a_varchar varchar) WITH (primary_key = ARRAY['a_varchar'])")) {
            assertThat(query("ALTER TABLE " + table.getName() + " ADD COLUMN b_varchar varchar NOT NULL"))
                    .failure().hasErrorCode(NOT_SUPPORTED);
            assertTableColumnNames(table.getName(), "a_varchar");
            assertThat(query("SELECT * FROM " + table.getName())).returnsEmptyResult();
        }
    }

    @Test
    @Override
    public void testAddColumnWithPosition()
    {
        try (TestTable table = newTrinoTable("test_add_column_", "WITH (primary_key = ARRAY['second']) AS SELECT 2 second, 4 fourth")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " ADD COLUMN first integer FIRST",
                    "This connector does not support adding columns with FIRST clause");
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " ADD COLUMN third integer AFTER second",
                    "This connector does not support adding columns with AFTER clause");
            assertTableColumnNames(table.getName(), "second", "fourth");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (2, 4)");
        }
    }

    @Test
    @Override
    public void testDropColumn()
    {
        String tableName;
        try (TestTable table = newTrinoTable("test_drop_column_", "WITH (primary_key = ARRAY['a']) AS SELECT 123 x, 456 y, 111 a")) {
            tableName = table.getName();
            assertThat(query("ALTER TABLE " + tableName + " DROP COLUMN a"))
                    .failure().hasErrorCode(NOT_SUPPORTED);
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN x");
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS y");
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
            assertQueryFails("SELECT x FROM " + tableName, ".* Column 'x' cannot be resolved");
            assertQueryFails("SELECT y FROM " + tableName, ".* Column 'y' cannot be resolved");
            assertQuery("SELECT a FROM " + tableName, "VALUES 111");
            assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN a", ".* Cannot drop the only column in a table");
        }

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    @Override
    public void testDropAndAddColumnWithSameName()
    {
        try (TestTable table = newTrinoTable("test_drop_add_column", "WITH (primary_key = ARRAY['x']) AS SELECT 1 x, 2 y, 3 z")) {
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3)");
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN y int");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3, NULL)");
        }
    }

    @Test
    @Override
    public void testDropNotNullConstraint()
    {
        try (TestTable table = newTrinoTable("test_drop_not_null_", "(id bigint NOT NULL, col integer NOT NULL) WITH (primary_key = ARRAY['id'])")) {
            assertThat(columnIsNullable(table.getName(), "col")).isFalse();
            assertThat(query("ALTER TABLE " + table.getName() + " ALTER COLUMN id DROP NOT NULL"))
                    .failure().hasErrorCode(NOT_SUPPORTED);
            assertThat(columnIsNullable(table.getName(), "id")).isFalse();

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col DROP NOT NULL");
            assertThat(columnIsNullable(table.getName(), "col")).isTrue();
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, NULL)", 1);
            assertQuery("SELECT id, col FROM " + table.getName(), "VALUES (CAST(1 AS BIGINT), NULL)");
        }
    }

    @Test
    @Override
    public void testRenameTable()
    {
        String tableName = "test_rename_" + randomNameSuffix();
        String renamedTable = "test_rename_new_" + randomNameSuffix();
        String testExistsTableName = "test_rename_exists_" + randomNameSuffix();
        String uppercaseName = "TEST_RENAME_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (primary_key = ARRAY['x']) AS SELECT 123 x", 1);
        try {
            assertUpdate("ALTER TABLE " + tableName + " RENAME TO " + renamedTable);
            assertQuery("SELECT x FROM " + renamedTable, "VALUES 123");
            assertUpdate("ALTER TABLE IF EXISTS " + renamedTable + " RENAME TO " + testExistsTableName);
            assertQuery("SELECT x FROM " + testExistsTableName, "VALUES 123");
            assertUpdate("ALTER TABLE " + testExistsTableName + " RENAME TO " + uppercaseName);
            assertQuery("SELECT x FROM " + uppercaseName.toLowerCase(ENGLISH), "VALUES 123");
            assertUpdate("DROP TABLE " + uppercaseName);

            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
            assertThat(getQueryRunner().tableExists(getSession(), renamedTable)).isFalse();
            assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME TO " + renamedTable);
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
            assertThat(getQueryRunner().tableExists(getSession(), renamedTable)).isFalse();
        }
        finally {
            for (String name : List.of(tableName, renamedTable, testExistsTableName, uppercaseName)) {
                assertUpdate("DROP TABLE IF EXISTS " + name);
            }
        }
    }

    @Test
    @Override
    public void testRenameTableToLongTableName()
    {
        int maxLength = maxTableRenameLength().orElseThrow();
        String baseTableName = "test_rename_target_" + randomNameSuffix();
        String validTargetTableName = baseTableName + "z".repeat(maxLength - baseTableName.length());
        String invalidTargetTableName = validTargetTableName + "z";

        String sourceTableName1 = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName1 + " WITH (primary_key = ARRAY['x']) AS SELECT 123 x", 1);
        try {
            assertUpdate("ALTER TABLE " + sourceTableName1 + " RENAME TO " + validTargetTableName);
            assertThat(getQueryRunner().tableExists(getSession(), validTargetTableName)).isTrue();
            assertQuery("SELECT x FROM " + validTargetTableName, "VALUES 123");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + validTargetTableName);
            assertUpdate("DROP TABLE IF EXISTS " + sourceTableName1);
        }

        String sourceTableName2 = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName2 + " WITH (primary_key = ARRAY['x']) AS SELECT 123 x", 1);
        try {
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + sourceTableName2 + " RENAME TO " + invalidTargetTableName))
                    .satisfies(this::verifyTableNameLengthFailurePermissible);
            assertThat(getQueryRunner().tableExists(getSession(), invalidTargetTableName)).isFalse();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + invalidTargetTableName);
            assertUpdate("DROP TABLE IF EXISTS " + sourceTableName2);
        }
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return requiresDelimiting(columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(BaseConnectorTest.DataMappingTestSetup dataMappingTestSetup)
    {
        String trinoTypeName = dataMappingTestSetup.getTrinoTypeName();
        if (trinoTypeName.equals("char(3)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        else if (trinoTypeName.equals("time")
                || trinoTypeName.equals("time(6)")
                || trinoTypeName.equals("timestamp(3) with time zone")
                || trinoTypeName.equals("timestamp(6) with time zone")) {
            // YQL has no TIME type.
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        if (dataMappingTestSetup.getTrinoTypeName().equals("char(1)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        // YQL strings have unbounded length.
        return MaterializedResult.resultBuilder(this.getSession(), new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR}).row(new Object[] {"orderkey", "bigint", "", ""}).row(new Object[] {"custkey", "bigint", "", ""}).row(new Object[] {"orderstatus", "varchar", "", ""}).row(new Object[] {"totalprice", "double", "", ""}).row(new Object[] {"orderdate", "date", "", ""}).row(new Object[] {"orderpriority", "varchar", "", ""}).row(new Object[] {"clerk", "varchar", "", ""}).row(new Object[] {"shippriority", "integer", "", ""}).row(new Object[] {"comment", "varchar", "", ""}).build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // YQL strings have unbounded length.
        String catalog = this.getSession().getCatalog().orElseThrow();
        String schema = this.getSession().getSchema().orElseThrow();
        Assertions.assertThat(this.computeScalar("SHOW CREATE TABLE orders")).isEqualTo(String.format("CREATE TABLE %s.%s.orders (\n   orderkey bigint,\n   custkey bigint,\n   orderstatus varchar,\n   totalprice double,\n   orderdate date,\n   orderpriority varchar,\n   clerk varchar,\n   shippriority integer,\n   comment varchar\n)\nWITH (\n   primary_key = ARRAY['orderkey']\n)", catalog, schema));
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e.getMessage()).contains("too long");
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e.getMessage()).contains("too long");
    }

    @Test
    public void testNativeTypeReads()
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = new TestTable(
                executor,
                "native_types_",
                "(id Int64, u Uint64, s Utf8, b String, d Date32, t Timestamp64, n Decimal(22,9), PRIMARY KEY (id))")) {
            executor.execute("INSERT INTO " + table.getName() + " (id, u, s, b, d, t, n) VALUES " +
                    "(1, 18446744073709551615ul, 'hello'u, String('\\x00\\x80\\xff'), Date32('1969-12-31'), " +
                    "Timestamp64('1969-12-31T23:59:59.999999Z'), Decimal('1.25', 22, 9)), " +
                    "(2, NULL, NULL, NULL, NULL, NULL, NULL)");
            assertQuery("SELECT id, u, s, b, d, t, n FROM " + table.getName(),
                    "VALUES (CAST(1 AS BIGINT), CAST('18446744073709551615' AS DECIMAL(20,0)), 'hello', X'0080FF', DATE '1969-12-31', " +
                            "TIMESTAMP '1969-12-31 23:59:59.999999', CAST('1.25' AS DECIMAL(22,9))), " +
                            "(CAST(2 AS BIGINT), NULL, NULL, NULL, NULL, NULL, NULL)");
        }
    }

    @Test
    public void testUnsupportedMutationsRejected()
    {
        Session defaultWriteSession = Session.builder(getSession())
                .setCatalogSessionProperty("local", "non_transactional_merge", "false")
                .build();
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = new TestTable(
                executor,
                "read_only_",
                "(id Int64, value Utf8, PRIMARY KEY (id))")) {
            executor.execute("INSERT INTO " + table.getName() + " (id, value) VALUES (1, 'original'u)");
            for (String sql : List.of(
                    "UPDATE " + table.getName() + " SET id = 2 WHERE id = 1",
                    "DELETE FROM " + table.getName() + " WHERE value LIKE '%g%'",
                    "MERGE INTO " + table.getName() + " t USING (VALUES (1, 'changed')) s(id, value) " +
                            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value",
                    "ALTER TABLE " + table.getName() + " RENAME COLUMN value TO renamed_value")) {
                assertThat(query(defaultWriteSession, sql)).failure().hasErrorCode(NOT_SUPPORTED);
            }
            assertQuery("SELECT id, value FROM " + table.getName(), "VALUES (CAST(1 AS BIGINT), 'original')");
        }
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
        String query = "SELECT name, nationkey, regionkey FROM nation";
        try (TestTable table = newTrinoTable("test_insert_", "(name varchar, nationkey bigint, regionkey bigint) " +
                "WITH (primary_key = ARRAY['nationkey', 'name', 'regionkey'])")) {
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 0");
            assertUpdate("INSERT INTO " + table.getName() + " " + query, 25);
            assertQuery("SELECT * FROM " + table.getName(), query);
            assertUpdate("INSERT INTO " + table.getName() + " (nationkey) VALUES (-1)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (nationkey) VALUES (null)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (name) VALUES ('name-dummy-1')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (nationkey, name) VALUES (-2, 'name-dummy-2')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (name, nationkey) VALUES ('name-dummy-3', -3)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (regionkey) VALUES (1234)", 1);
            String expected = query + " UNION ALL SELECT null, -1, null UNION ALL SELECT null, null, null " +
                    "UNION ALL SELECT 'name-dummy-1', null, null UNION ALL SELECT 'name-dummy-2', -2, null " +
                    "UNION ALL SELECT 'name-dummy-3', -3, null UNION ALL SELECT null, null, 1234";
            assertQuery("SELECT * FROM " + table.getName(), expected);
            // YDB's explicit primary key rejects duplicate rows instead of creating an unkeyed bag.
            assertThat(query("INSERT INTO " + table.getName() + " (nationkey, name, regionkey) " +
                    "SELECT nationkey, name, regionkey FROM nation UNION ALL SELECT nationkey, name, regionkey FROM nation"))
                    .failure().hasErrorCode(JDBC_ERROR);
            assertQuery("SELECT * FROM " + table.getName(), expected);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb)),
                "test_insert_default_",
                "(col_required Int64 NOT NULL, " +
                        "col_nullable Int64, " +
                        "col_default Int64 DEFAULT 43, " +
                        "col_nonnull_default Int64 NOT NULL DEFAULT 42, " +
                        "col_required2 Int64 NOT NULL, " +
                        "PRIMARY KEY (col_required))");
    }

    @Test
    @Override
    public void testInsertUnicode()
    {
        try (TestTable table = newTrinoTable("test_insert_unicode_", "(test varchar(50)) WITH (primary_key = ARRAY['test'])")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5world\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试world编码");
        }

        try (TestTable table = newTrinoTable("test_insert_unicode_", "(test varchar(50)) WITH (primary_key = ARRAY['test'])")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'aa', 'bé'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'aa', 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'aa'", "VALUES 'aa'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'ba'", "VALUES 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'ba'", "VALUES 'aa'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'ba'");
        }

        try (TestTable table = newTrinoTable("test_insert_unicode_", "(test varchar(50)) WITH (primary_key = ARRAY['test'])")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'a', 'é'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'a', 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'a'", "VALUES 'a'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'b'", "VALUES 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'b'", "VALUES 'a'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'b'");
        }
    }

    @Test
    @Override
    public void testInsertHighestUnicodeCharacter()
    {
        try (TestTable table = newTrinoTable("test_insert_unicode_", "(test varchar(50)) WITH (primary_key = ARRAY['test'])")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
        }
    }

    @Override
    protected TestTable createTableWithOneIntegerColumn(String namePrefix)
    {
        return newTrinoTable(namePrefix, "(col integer) WITH (primary_key = ARRAY['col'])");
    }

    @Test
    @Override
    public void testInsertArray()
    {
        assertThat(query("CREATE TABLE insert_array_unsupported (id bigint, value array(bigint)) WITH (primary_key = ARRAY['id'])"))
                .failure().hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        try (TestTable table = newTrinoTable("insert_date", "(id bigint, dt date) WITH (primary_key = ARRAY['id'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, DATE '-0001-01-01')", 1);
            assertQuery("SELECT dt FROM " + table.getName(), "VALUES DATE '-0001-01-01'");
            assertQuery("SELECT dt FROM " + table.getName() + " WHERE dt = DATE '-0001-01-01'", "VALUES DATE '-0001-01-01'");
        }
    }
}
