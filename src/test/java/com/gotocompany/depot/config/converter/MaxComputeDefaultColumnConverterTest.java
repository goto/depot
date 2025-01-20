package com.gotocompany.depot.config.converter;

import com.aliyun.odps.Column;
import org.junit.Test;

import java.util.List;

public class MaxComputeDefaultColumnConverterTest {

    private static final MaxComputeDefaultColumnConverter converter = new MaxComputeDefaultColumnConverter();

    @Test
    public void shouldParseComplexTableStatement() {
        String input = "col1:STRING,col2:ARRAY<STRING>,col3:STRUCT<col31:STRING,col32:STRUCT<col321:STRING,col322:STRING>>>";

        List<Column> columns = converter.convert(null, input);

        System.out.println(columns);
    }
}
