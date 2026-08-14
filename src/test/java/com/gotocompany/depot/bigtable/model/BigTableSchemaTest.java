package com.gotocompany.depot.bigtable.model;

import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link BigTableSchema}, the parsed view of the Bigtable column-family mapping.
 *
 * <p>The mapping is supplied through the {@code SINK_BIGTABLE_COLUMN_FAMILY_MAPPING} system property
 * and loaded into a {@link BigTableSinkConfig} via {@code ConfigFactory}; {@link #setUp()} seeds a
 * default two-family mapping. Individual tests override that property to cover empty, partial and
 * malformed mappings before constructing a fresh {@link BigTableSchema}. Lookups for column families,
 * columns, bound source fields and missing families are then asserted, including the
 * {@link ConfigurationException} and {@link org.json.JSONException} error paths.</p>
 */
public class BigTableSchemaTest {
    /** Schema under test, rebuilt from the configured column-family mapping. */
    private BigTableSchema bigtableSchema;

    /**
     * Seeds a default two-family column mapping and builds the schema before each test.
     *
     * <p>Sets {@code SINK_BIGTABLE_COLUMN_FAMILY_MAPPING} to a mapping with {@code family_name1} and
     * {@code family_name2} (two qualifiers each), loads it into a {@link BigTableSinkConfig} and
     * constructs the {@link BigTableSchema} under test. Individual tests may override the property to
     * exercise other mappings.</p>
     */
    @Before
    public void setUp() {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{\n"
                + "\"family_name1\" : {\n"
                + "\"qualifier_name1\" : \"data.is_complete\",\n"
                + "\"qualifier_name2\" : \"data.content\"\n"
                + "},\n"
                + "\"family_name2\" : {\n"
                + "\"qualifier_name3\" : \"base_content3\",\n"
                + "\"qualifier_name4\" : \"base_content4\"\n"
                + "}\n"
                + "}");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
    }

    /**
     * Verifies that the configured column families are exposed as a set.
     *
     * <p>Asserts that {@link BigTableSchema#getColumnFamilies()} returns exactly the two seeded
     * families, {@code family_name1} and {@code family_name2}.</p>
     */
    @Test
    public void shouldGeSetOfColumnFamilies() {
        Set<String> columnFamilies = bigtableSchema.getColumnFamilies();
        assertEquals(2, columnFamilies.size());
        assertTrue(columnFamilies.contains("family_name1"));
        assertTrue(columnFamilies.contains("family_name2"));
    }

    /**
     * Verifies that a column family and qualifier resolve to their configured source field.
     *
     * <p>Asserts that {@link BigTableSchema#getField(String, String)} returns the source field bound
     * to each qualifier across both seeded families.</p>
     */
    @Test
    public void shouldGetFieldNameForGivenColumnFamilyAndQualifier() {
        assertEquals("data.is_complete", bigtableSchema.getField("family_name1", "qualifier_name1"));
        assertEquals("data.content", bigtableSchema.getField("family_name1", "qualifier_name2"));

        assertEquals("base_content3", bigtableSchema.getField("family_name2", "qualifier_name3"));
        assertEquals("base_content4", bigtableSchema.getField("family_name2", "qualifier_name4"));
    }

    /**
     * Verifies that the qualifiers configured within a family are exposed as a set.
     *
     * <p>Asserts that {@link BigTableSchema#getColumns(String)} returns the two qualifiers configured
     * for each of the two seeded families.</p>
     */
    @Test
    public void shouldGetColumnsForGivenColumnFamily() {
        assertEquals(2, bigtableSchema.getColumns("family_name1").size());
        assertTrue(bigtableSchema.getColumns("family_name1").contains("qualifier_name1"));
        assertTrue(bigtableSchema.getColumns("family_name1").contains("qualifier_name2"));

        assertEquals(2, bigtableSchema.getColumns("family_name2").size());
        assertTrue(bigtableSchema.getColumns("family_name2").contains("qualifier_name3"));
        assertTrue(bigtableSchema.getColumns("family_name2").contains("qualifier_name4"));
    }

    /**
     * Verifies that an empty mapping is rejected at construction time.
     *
     * <p>Given an empty {@code SINK_BIGTABLE_COLUMN_FAMILY_MAPPING}, when a {@link BigTableSchema} is
     * constructed, then a {@link ConfigurationException} with the message
     * {@code "Column Mapping should not be empty or null"} is thrown.</p>
     */
    @Test
    public void shouldThrowConfigurationExceptionWhenColumnMappingIsEmpty() {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        ConfigurationException configurationException = assertThrows(ConfigurationException.class, () -> new BigTableSchema(sinkConfig.getColumnFamilyMapping()));
        Assert.assertEquals("Column Mapping should not be empty or null", configurationException.getMessage());
    }

    /**
     * Verifies that an empty JSON object mapping yields no column families.
     *
     * <p>Given a mapping of an empty JSON object, when the schema is built, then
     * {@link BigTableSchema#getColumnFamilies()} returns an empty set.</p>
     */
    @Test
    public void shouldReturnEmptySetIfNoColumnFamilies() {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{}");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        Set<String> columnFamilies = bigtableSchema.getColumnFamilies();
        Assert.assertEquals(0, columnFamilies.size());
    }

    /**
     * Verifies that a family declared with no qualifiers exposes an empty column set.
     *
     * <p>Given a mapping where {@code family_name2} maps to an empty object, when the schema is built,
     * then both families are reported and {@link BigTableSchema#getColumns(String)} for
     * {@code family_name2} returns an empty set.</p>
     */
    @Test
    public void shouldReturnEmptySetIfNoColumnsPresent() {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{\n"
                + "\"family_name1\" : {\n"
                + "\"qualifier_name1\" : \"data.is_complete\",\n"
                + "\"qualifier_name2\" : \"data.content\"\n"
                + "},\n"
                + "\"family_name2\" : {}\n"
                + "}");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        Set<String> columnFamilies = bigtableSchema.getColumnFamilies();
        Assert.assertEquals(2, columnFamilies.size());
        Set<String> columns = bigtableSchema.getColumns("family_name2");
        Assert.assertEquals(0, columns.size());
    }

    /**
     * Verifies that lookups for absent families or qualifiers raise a JSON error.
     *
     * <p>Asserts that {@link BigTableSchema#getColumns(String)} for an unknown family and
     * {@link BigTableSchema#getField(String, String)} for an unknown qualifier each throw a
     * {@link org.json.JSONException} whose message names the missing key.</p>
     */
    @Test
    public void shouldThrowJsonException() {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{\n"
                + "\"family_name1\" : {\n"
                + "\"qualifier_name1\" : \"data.is_complete\",\n"
                + "\"qualifier_name2\" : \"data.content\"\n"
                + "},\n"
                + "\"family_name2\" : {}\n"
                + "}");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        Set<String> columnFamilies = bigtableSchema.getColumnFamilies();
        Assert.assertEquals(2, columnFamilies.size());
        JSONException jsonException = assertThrows(JSONException.class, () -> bigtableSchema.getColumns("family_name3"));
        Assert.assertEquals("JSONObject[\"family_name3\"] not found.", jsonException.getMessage());

        jsonException = assertThrows(JSONException.class, () -> bigtableSchema.getField("family_name1", "qualifier_name3"));
        Assert.assertEquals("JSONObject[\"qualifier_name3\"] not found.", jsonException.getMessage());
    }

    /**
     * Verifies that no families are reported missing when all configured families are present.
     *
     * <p>Given an existing-family set equal to the configured families, when
     * {@link BigTableSchema#getMissingColumnFamilies(java.util.Set)} is called, then the returned set
     * is empty.</p>
     */
    @Test
    public void shouldReturnEmptySetOfMissingColumnFamilies() {
        Set<String> missingColumnFamilies = bigtableSchema.getMissingColumnFamilies(new HashSet<String>() {{
            add("family_name1");
            add("family_name2");
        }});
        Assert.assertEquals(0, missingColumnFamilies.size());
    }

    /**
     * Verifies that configured families absent from the supplied set are reported as missing.
     *
     * <p>Given an existing-family set of {@code family_name3}, {@code family_name2} and
     * {@code family_name4}, when {@link BigTableSchema#getMissingColumnFamilies(java.util.Set)} is
     * called, then it returns a singleton set containing {@code family_name1} — the configured family
     * not present in the input.</p>
     */
    @Test
    public void shouldReturnMissingColumnFamilies() {
        Set<String> missingColumnFamilies = bigtableSchema.getMissingColumnFamilies(new HashSet<String>() {{
            add("family_name3");
            add("family_name2");
            add("family_name4");
        }});
        Assert.assertEquals(new HashSet<String>() {{
            add("family_name1");
        }}, missingColumnFamilies);
    }
}
