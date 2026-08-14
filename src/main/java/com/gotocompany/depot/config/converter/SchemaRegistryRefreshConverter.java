package com.gotocompany.depot.config.converter;

import com.gotocompany.stencil.cache.SchemaRefreshStrategy;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that selects a Stencil {@link SchemaRefreshStrategy} based on a
 * configuration property value.
 *
 * <p>The single recognised token is {@code VERSION_BASED_REFRESH} (matched case-insensitively),
 * which yields a version-based refresh strategy; any other value, including {@code null}, falls back
 * to the long-polling strategy. The chosen strategy governs how the Stencil schema cache decides
 * when to fetch updated descriptors.
 *
 * @see SchemaRefreshStrategy
 * @see Converter
 */
public class SchemaRegistryRefreshConverter implements Converter<SchemaRefreshStrategy> {

    /**
     * Selects the {@link SchemaRefreshStrategy} that corresponds to the configured value.
     *
     * <p>If the input equals {@code "VERSION_BASED_REFRESH"} ignoring case, the version-based refresh
     * strategy is returned; otherwise, for every other value (including {@code null}), the
     * long-polling strategy is returned.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value selecting the refresh strategy; may be {@code null}
     * @return {@link SchemaRefreshStrategy#versionBasedRefresh()} when {@code input} is
     *         {@code "VERSION_BASED_REFRESH"} ignoring case, otherwise
     *         {@link SchemaRefreshStrategy#longPollingStrategy()}
     */
    @Override
    public SchemaRefreshStrategy convert(Method method, String input) {
        if ("VERSION_BASED_REFRESH".equalsIgnoreCase(input)) {
            return SchemaRefreshStrategy.versionBasedRefresh();
        }
        return SchemaRefreshStrategy.longPollingStrategy();
    }
}
