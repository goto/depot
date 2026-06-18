package com.gotocompany.depot.config.preprocessor;

import org.aeonbits.owner.Preprocessor;

/**
 * Owner {@link Preprocessor} that strips leading and trailing whitespace from raw configuration
 * values.
 *
 * <p>The Aeon Owner library applies preprocessors to the textual value of a property before it is
 * passed to any converter and returned from a config accessor. Registering this preprocessor, via
 * {@code @Config.PreprocessorClasses({Trim.class})} as done on
 * {@link com.gotocompany.depot.config.RedisSinkConfig}, ensures that incidental whitespace around a
 * configured value, for example a stray trailing space in an environment variable, does not leak into
 * parsed configuration.
 */
public class Trim implements Preprocessor {
    /**
     * Trims surrounding whitespace from the given configuration value.
     *
     * <p>A {@code null} input is propagated unchanged so that the absence of a value remains
     * distinguishable from an empty string.
     *
     * @param input the raw configuration value to preprocess; may be {@code null}
     * @return {@code null} if {@code input} is {@code null}; otherwise {@code input} with leading and
     *         trailing whitespace removed via {@link String#trim()}
     */
    @Override
    public String process(String input) {
        return input == null ? null : input.trim();
    }
}
