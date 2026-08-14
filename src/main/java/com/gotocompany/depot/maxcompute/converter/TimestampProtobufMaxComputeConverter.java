package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import com.gotocompany.depot.maxcompute.util.LocalDateTimeValidator;

import java.sql.Timestamp;

/**
 * {@link ProtobufMaxComputeConverter} that maps a {@code google.protobuf.Timestamp} field onto the MaxCompute
 * {@code TIMESTAMP} type.
 *
 * <p>The {@code seconds} and {@code nanos} components of the protobuf timestamp are validated and assembled into
 * a {@link java.time.LocalDateTime} by a {@link LocalDateTimeValidator} (which applies the configured time zone,
 * range checks, optional truncation, and partition-key checks), and the result is returned as a
 * {@link java.sql.Timestamp}. When ignoring negative-second timestamps is enabled, values whose {@code seconds}
 * component is negative are converted to {@code null} instead.</p>
 *
 * @see TimestampNTZProtobufMaxComputeConverter
 * @see LocalDateTimeValidator
 */
public class TimestampProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    /**
     * Name of the {@code seconds} field within the protobuf timestamp message.
     */
    private static final String SECONDS = "seconds";
    /**
     * Name of the {@code nanos} field within the protobuf timestamp message.
     */
    private static final String NANOS = "nanos";

    /**
     * Validator that range-checks the epoch and assembles it into a {@link java.time.LocalDateTime}.
     */
    private final LocalDateTimeValidator localDateTimeValidator;
    /**
     * When {@code true}, timestamps with a negative {@code seconds} component are converted to {@code null}.
     */
    private final boolean isIgnoreNegativeSecondTimestampEnabled;

    /**
     * Creates the converter from configuration.
     *
     * @param maxComputeSinkConfig the sink configuration providing the time-zone, range, truncation, and
     *                             negative-second handling settings used to validate timestamps
     */
    public TimestampProtobufMaxComputeConverter(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.localDateTimeValidator = new LocalDateTimeValidator(maxComputeSinkConfig);
        this.isIgnoreNegativeSecondTimestampEnabled = maxComputeSinkConfig.isIgnoreNegativeSecondTimestampEnabled();
    }

    /**
     * Returns the MaxCompute {@code TIMESTAMP} type used to store the value.
     *
     * @param protoPayload the payload wrapper for the field; not inspected because the type is always {@code TIMESTAMP}
     * @return the MaxCompute {@code TIMESTAMP} {@link TypeInfo}
     */
    @Override
    public TypeInfo convertSingularTypeInfo(ProtoPayload protoPayload) {
        return TypeInfoFactory.TIMESTAMP;
    }

    /**
     * Converts a protobuf timestamp value into a MaxCompute timestamp.
     *
     * <p>Reads the {@code seconds} and {@code nanos} fields of the message; if negative-second timestamps are
     * ignored and {@code seconds} is negative the method returns {@code null}. Otherwise the epoch is validated
     * and assembled into a {@link java.time.LocalDateTime} (using the field name and root-level flag for
     * partition-key validation) and returned as a {@link java.sql.Timestamp}.</p>
     *
     * @param protoPayload the payload wrapper carrying the parsed timestamp message and field descriptor
     * @return the converted {@link java.sql.Timestamp}, or {@code null} when a negative-second timestamp is ignored
     * @throws com.gotocompany.depot.exception.InvalidMessageException if the timestamp fails range or partition-key validation
     */
    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        Message message = (Message) protoPayload.getParsedObject();
        long seconds = (long) message.getField(message.getDescriptorForType().findFieldByName(SECONDS));
        if (seconds < 0 && isIgnoreNegativeSecondTimestampEnabled) {
            return null;
        }
        int nanos = (int) message.getField(message.getDescriptorForType().findFieldByName(NANOS));
        return Timestamp.valueOf(localDateTimeValidator.parseAndValidate(seconds, nanos, protoPayload.getFieldDescriptor().getName(), protoPayload.isRootLevel()));
    }

}
