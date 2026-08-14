package com.gotocompany.depot.config.converter;


import com.gotocompany.depot.config.enums.SinkConnectorSchemaDataType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves a configuration property value into a
 * {@link SinkConnectorSchemaDataType} constant.
 *
 * <p>This converter is registered against the sink connector schema configuration through the
 * Aeon Owner {@code @ConverterClass} mechanism. It selects the wire format Depot expects for
 * incoming records, exposing the choice as a strongly typed enum rather than as a raw
 * {@link String}.
 *
 * <p>The accepted tokens correspond to the names of the {@link SinkConnectorSchemaDataType}
 * constants ({@code PROTOBUF} and {@code JSON}) and are matched case-insensitively.
 *
 * @see SinkConnectorSchemaDataType
 * @see Converter
 */
public class SinkConnectorSchemaDataTypeConverter implements Converter<SinkConnectorSchemaDataType> {
    /**
     * Converts the raw configuration value into the matching {@link SinkConnectorSchemaDataType} constant.
     *
     * <p>The supplied value is upper-cased using the platform default locale and then resolved
     * through {@link SinkConnectorSchemaDataType#valueOf(String)}, which makes the lookup
     * effectively case-insensitive while still requiring an otherwise exact match of a declared
     * constant name.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value to convert, expected to name a
     *               {@link SinkConnectorSchemaDataType} constant
     * @return the {@link SinkConnectorSchemaDataType} constant whose name matches {@code input}
     *         ignoring case
     * @throws IllegalArgumentException if {@code input} does not match any declared constant
     * @throws NullPointerException     if {@code input} is {@code null}
     */
    @Override
    public SinkConnectorSchemaDataType convert(Method method, String input) {
        return SinkConnectorSchemaDataType.valueOf(input.toUpperCase());
    }
}
