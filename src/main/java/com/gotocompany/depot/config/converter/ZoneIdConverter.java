package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.exception.ConfigurationException;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.time.ZoneId;

/**
 * Owner {@link Converter} that parses a configuration property value into a {@link ZoneId}.
 *
 * <p>The value is resolved via {@link ZoneId#of(String)}, accepting fixed offsets (for example
 * {@code +05:30}) as well as region identifiers (for example {@code Asia/Kolkata}). Any failure to
 * resolve the value is wrapped in a {@link ConfigurationException} so that the misconfiguration is
 * reported consistently with the rest of Depot's configuration handling.
 *
 * @see Converter
 * @see ZoneId#of(String)
 */
public class ZoneIdConverter implements Converter<ZoneId> {

    /**
     * Parses the configuration value into a {@link ZoneId}.
     *
     * <p>Delegates to {@link ZoneId#of(String)} and, if that throws for any reason (for example an
     * unknown region, a malformed offset, or a {@code null} value), rethrows the failure wrapped in a
     * {@link ConfigurationException} whose message includes the offending input.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param s      the raw property value, expected to be a zone offset or region identifier
     * @return the {@link ZoneId} resolved from {@code s}
     * @throws ConfigurationException if {@code s} cannot be resolved to a valid {@link ZoneId}
     */
    @Override
    public ZoneId convert(Method method, String s) {
        try {
            return ZoneId.of(s);
        } catch (Exception e) {
            throw new ConfigurationException("Invalid ZoneId: " + s, e);
        }
    }

}
