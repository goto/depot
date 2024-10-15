package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TextMaxComputeTable;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.sun.tools.javac.util.List;
import org.assertj.core.api.Assertions;
import org.assertj.core.groups.Tuple;
import org.junit.Test;
import org.mockito.Mockito;


public class TableUtilTest {

    private final Descriptors.Descriptor descriptor = TextMaxComputeTable.Table.getDescriptor();

    @Test
    public void shouldBuildTableSchema() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("meta");
        Mockito.when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                List.of(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        int expectedNonPartitionColumnCount = 4;
        int expectedPartitionColumnCount = 1;

        TableSchema tableSchema = TableUtil.buildTableSchema(
                descriptor,
                maxComputeSinkConfig
        );

        Assertions.assertThat(tableSchema.getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        Assertions.assertThat(tableSchema.getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        Assertions.assertThat(tableSchema.getColumns())
                .extracting("name", "typeInfo")
                .containsExactlyInAnyOrder(
                        Tuple.tuple("id", TypeInfoFactory.STRING),
                        Tuple.tuple("user", TypeInfoFactory.getStructTypeInfo(
                                List.of("id", "contacts"),
                                List.of(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                        List.of("number"),
                                        List.of(TypeInfoFactory.STRING)
                                )))
                        )),
                        Tuple.tuple("items", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                List.of("id", "name"),
                                List.of(TypeInfoFactory.STRING, TypeInfoFactory.STRING)
                        ))),
                        Tuple.tuple("meta", TypeInfoFactory.getStructTypeInfo(
                                List.of("__message_timestamp", "__kafka_topic", "__kafka_offset"),
                                List.of(TypeInfoFactory.TIMESTAMP_NTZ, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT)
                        ))
                );
        Assertions.assertThat(tableSchema.getPartitionColumns())
                .extracting("name", "typeInfo")
                .contains(Tuple.tuple("event_timestamp", TypeInfoFactory.TIMESTAMP_NTZ));
    }
}
