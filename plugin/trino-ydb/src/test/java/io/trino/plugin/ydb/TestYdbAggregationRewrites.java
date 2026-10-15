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
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestYdbAggregationRewrites
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
    public void testCountAndExtrema()
    {
        assertThat(client.implementAggregation(SESSION, new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty()), Map.of()))
                .get().extracting(expression -> expression.getExpression()).isEqualTo("count(*)");
        for (Type type : List.of(BIGINT, VARCHAR)) {
            JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(type).orElseThrow(), type);
            for (String function : List.of("count", "min", "max")) {
                Type outputType = function.equals("count") ? BIGINT : type;
                assertThat(client.implementAggregation(SESSION, aggregate(function, outputType, type, false), Map.of("v", column)))
                        .get().extracting(expression -> expression.getExpression()).isEqualTo(function + "(`value`)");
            }
            assertThat(client.implementAggregation(SESSION, aggregate("count", BIGINT, type, true), Map.of("v", column)))
                    .get().extracting(expression -> expression.getExpression()).isEqualTo("count(DISTINCT `value`)");
        }
    }

    @Test
    public void testFloatingPointRestrictions()
    {
        for (Type type : List.of(REAL, DOUBLE)) {
            JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(type).orElseThrow(), type);
            for (String function : List.of("sum", "avg")) {
                assertThat(client.implementAggregation(SESSION, aggregate(function, type, type, false), Map.of("v", column))).isPresent();
                assertThat(client.implementAggregation(SESSION, aggregate(function, type, type, true), Map.of("v", column))).isEmpty();
            }
            for (String function : List.of("min", "max")) {
                assertThat(client.implementAggregation(SESSION, aggregate(function, type, type, false), Map.of("v", column))).isEmpty();
            }
            assertThat(client.implementAggregation(SESSION, aggregate("count", BIGINT, type, true), Map.of("v", column))).isEmpty();
            assertThat(client.implementAggregation(SESSION, aggregate("count", BIGINT, type, false), Map.of("v", column))).isPresent();
        }
    }

    @Test
    public void testIntegralAndDecimalArithmeticRemainInTrino()
    {
        for (Type type : List.of(BIGINT, createDecimalType(22, 9))) {
            JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(type).orElseThrow(), type);
            for (String function : List.of("sum", "avg")) {
                assertThat(client.implementAggregation(SESSION, aggregate(function, type, type, false), Map.of("v", column))).isEmpty();
            }
        }
        Type decimal = createDecimalType(22, 9);
        JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(decimal).orElseThrow(), decimal);
        assertThat(client.implementAggregation(SESSION, aggregate("count", BIGINT, decimal, false), Map.of("v", column))).isEmpty();
    }

    @Test
    public void testGroupingTypeRestrictions()
    {
        JdbcTableHandle table = new JdbcTableHandle(
                new SchemaTableName("default", "test"),
                new RemoteTableName(Optional.empty(), Optional.empty(), "test"),
                Optional.empty());
        assertThat(client.supportsAggregationPushdown(SESSION, table, List.of(), Map.of(), List.of(List.of()))).isTrue();
        for (Type type : List.of(BIGINT, VARCHAR, REAL, DOUBLE, createDecimalType(22, 9))) {
            JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(type).orElseThrow(), type);
            assertThat(client.supportsAggregationPushdown(SESSION, table, List.of(), Map.of("v", column), List.of(List.of(column))))
                    .isEqualTo(type.equals(BIGINT) || type.equals(VARCHAR));
        }
    }

    @Test
    public void testUnsupportedMappingAndFunction()
    {
        JdbcColumnHandle column = new JdbcColumnHandle(
                "value",
                new JdbcTypeHandle(Types.BIGINT, Optional.of("UnexpectedNativeType"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                BIGINT);
        assertThat(client.implementAggregation(SESSION, aggregate("count", BIGINT, BIGINT, false), Map.of("v", column))).isEmpty();
        JdbcColumnHandle supported = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(DOUBLE).orElseThrow(), DOUBLE);
        for (String function : List.of("stddev", "variance", "corr", "regr_slope")) {
            assertThat(client.implementAggregation(SESSION, aggregate(function, DOUBLE, DOUBLE, false), Map.of("v", supported))).isEmpty();
        }
    }

    private static AggregateFunction aggregate(String function, Type outputType, Type argumentType, boolean distinct)
    {
        return new AggregateFunction(function, outputType, List.of(new Variable("v", argumentType)), List.of(), distinct, Optional.empty());
    }
}
