package com.gotocompany.depot.common;

import com.google.common.base.Splitter;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.utils.StringUtils;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

/**
 * Compiled representation of a Depot string template that interpolates message field values.
 *
 * <p>A template is supplied as a single comma-separated string whose first segment is a
 * {@link String#format(String, Object...)}-style pattern and whose remaining segments are the names
 * of message fields supplying the values for that pattern's placeholders. For example,
 * {@code "%s-%s, order_id, customer_id"} renders the {@code order_id} and {@code customer_id} field
 * values joined by a hyphen.
 *
 * <p>Commas that are part of the pattern itself can be escaped as {@code /,/} so they are not treated
 * as segment separators; such occurrences are unescaped back to a literal comma when the template is
 * compiled. The constructor validates that the number of placeholders in the pattern matches the
 * number of supplied field names. Lombok's {@link EqualsAndHashCode} derives equality from the
 * compiled pattern and field-name list.
 *
 * <p>Templates are used throughout Depot to build dynamic values such as table names, keys and tags
 * from the contents of each message.
 */
@EqualsAndHashCode
public class Template {
    /**
     * The {@link String#format(String, Object...)}-style pattern parsed from the first template
     * segment, with any escaped commas restored to literal commas.
     */
    private final String templatePattern;
    /**
     * The ordered names of the message fields whose values are substituted into the pattern's
     * placeholders; empty when the template is a constant string.
     */
    private final List<String> patternVariableFieldNames;

    /**
     * Compiles a template from its comma-separated string form.
     *
     * <p>The input is split on commas that are not escaped as {@code /,/}: the first segment becomes
     * the format pattern (with escaped commas restored) and the remaining segments become the field
     * names supplying the pattern's values. Each segment is trimmed of surrounding whitespace. The
     * compiled template is then validated via {@link #validate()}.
     *
     * @param template the raw template string; must be non-{@code null} and non-empty
     * @throws InvalidTemplateException if {@code template} is {@code null} or empty, or if the number
     *                                  of placeholders in the pattern does not match the number of
     *                                  supplied field names
     */
    public Template(String template) throws InvalidTemplateException {
        if (template == null || template.isEmpty()) {
            throw new InvalidTemplateException("Template cannot be empty");
        }
        List<String> templateStrings = new ArrayList<>();

        Splitter.onPattern("(?<!/,),(?!/)").omitEmptyStrings().split(template).forEach(s -> templateStrings.add(s.trim()));
        this.templatePattern = templateStrings.get(0).replaceAll("/,/", ",");
        this.patternVariableFieldNames = templateStrings.subList(1, templateStrings.size());
        validate();
    }

    /**
     * Validates that the pattern's placeholders align with the supplied field names.
     *
     * <p>Compares the number of valid format arguments and the number of {@code %} characters in the
     * pattern against the number of field names; all three counts must be equal for the template to be
     * considered valid.
     *
     * @throws InvalidTemplateException if the counts of {@code %} characters, valid format arguments
     *                                  and field names are not all equal
     */
    private void validate() throws InvalidTemplateException {
        int validArgs = StringUtils.countVariables(templatePattern);
        int values = patternVariableFieldNames.size();
        int variables = StringUtils.count(templatePattern, '%');
        if (validArgs != values || variables != values) {
            throw new InvalidTemplateException(String.format("Template is not valid, variables=%d, validArgs=%d, values=%d", variables, validArgs, values));
        }
    }

    /**
     * Renders the template against a parsed message, returning the interpolated string.
     *
     * <p>Each configured field name is resolved against {@code parsedMessage}, converted to its string
     * form, and substituted into the pattern via {@link String#format(String, Object...)}.
     *
     * @param parsedMessage the message supplying the field values referenced by this template
     * @return the formatted string with all field values substituted into the pattern
     */
    public String parse(ParsedMessage parsedMessage) {
        Object[] patternVariableData = patternVariableFieldNames
                .stream()
                .map(fieldName -> parsedMessage.getFieldByName(fieldName).toString())
                .toArray();
        return String.format(templatePattern, patternVariableData);
    }

    /**
     * Renders the template against a parsed message, preserving the original value type when possible.
     *
     * <p>When the template references at least one field and its pattern is exactly {@code "%s"}, the
     * raw value of the first referenced field is returned as-is (retaining its runtime type) rather
     * than being converted to a string. When the pattern is more elaborate, the fully interpolated
     * string from {@link #parse(ParsedMessage)} is returned. When the template references no fields,
     * the constant pattern string is returned unchanged.
     *
     * @param parsedMessage the message supplying the field values referenced by this template
     * @return the raw first field value for a single {@code "%s"} template, the interpolated string
     *         for a multi-token template, or the constant pattern when no fields are referenced
     */
    public Object parseWithType(ParsedMessage parsedMessage) {
        if (!patternVariableFieldNames.isEmpty()) {
            if (templatePattern.equals("%s")) {
                return parsedMessage.getFieldByName(patternVariableFieldNames.get(0));
            } else {
                return parse(parsedMessage);
            }
        }
        return templatePattern;
    }

    /**
     * Returns the compiled format pattern of this template.
     *
     * @return the pattern string, with escaped commas already restored to literal commas
     */
    public String getTemplateString() {
        return templatePattern;
    }

    /**
     * Indicates whether this template is a constant string with no field substitutions.
     *
     * @return {@code true} if the template references no message fields, {@code false} otherwise
     */
    public boolean isConstantString() {
        return patternVariableFieldNames.isEmpty();
    }
}
