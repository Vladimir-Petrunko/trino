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
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestYdbTopN
{
    private final YdbClient client = new YdbClient(
            new BaseJdbcConfig(),
            _ -> {
                throw new SQLException("This test must not open a connection");
            },
            new DefaultQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    @Test
    public void testFloatingPointOrderingMatchesTrinoOperators()
            throws Throwable
    {
        for (Type type : List.of(REAL, DOUBLE)) {
            String nativeType = type.equals(REAL) ? "Float" : "Double";
            JdbcColumnHandle column = new JdbcColumnHandle("key", YdbTypeUtils.toTypeHandle(type).orElseThrow(), type);
            var values = type.createBlockBuilder(null, 2);
            if (type.equals(REAL)) {
                type.writeLong(values, Float.floatToRawIntBits(Float.NaN));
                type.writeLong(values, Float.floatToRawIntBits(1.0f));
            }
            else {
                type.writeDouble(values, Double.NaN);
                type.writeDouble(values, 1.0);
            }
            var block = values.build();
            for (SortOrder order : SortOrder.values()) {
                int comparison = (int) new TypeOperators().getOrderingOperator(
                        type,
                        order,
                        simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)).invokeWithArguments(block, 0, block, 1);
                String topN = client.topNFunction().orElseThrow().apply(
                        "SELECT `key` FROM `test`",
                        List.of(new JdbcSortItem(column, order)),
                        2);
                assertThat(topN)
                        .contains(
                                "CASE WHEN `key` IS NULL THEN " + (order.isNullsFirst() ? "0 ELSE 1" : "1 ELSE 0") + " END ASC",
                                "CASE WHEN `key` != `key` THEN 1 ELSE 0 END " + (comparison < 0 ? "DESC" : "ASC"),
                                "NANVL(`key`, CAST(0 AS " + nativeType + ")) " + (order.isAscending() ? "ASC" : "DESC"))
                        .endsWith(" LIMIT 2");
            }
        }
    }

    @Test
    public void testQuotedIntegralSortAndLimit()
    {
        JdbcColumnHandle column = new JdbcColumnHandle("key space", YdbTypeUtils.toTypeHandle(BIGINT).orElseThrow(), BIGINT);
        assertThat(client.topNFunction().orElseThrow().apply(
                "SELECT `key space` FROM `test`",
                List.of(new JdbcSortItem(column, SortOrder.DESC_NULLS_FIRST)),
                3))
                .isEqualTo("SELECT `key space` FROM `test` ORDER BY CASE WHEN `key space` IS NULL THEN 0 ELSE 1 END ASC, `key space` DESC LIMIT 3");
        assertThat(client.limitFunction().orElseThrow().apply("SELECT `key space` FROM `test`", 3L))
                .isEqualTo("SELECT `key space` FROM `test` LIMIT 3");
    }
}
