package com.gotocompany.depot.bigtable.model;

import com.gotocompany.depot.exception.ConfigurationException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

/**
 * Parsed representation of the Bigtable column-family mapping configured for the sink.
 *
 * <p>The mapping is supplied as a JSON document whose top-level keys are column families, each mapping
 * to an object of column name to source field name. {@code BigTableSchema} wraps that JSON and exposes
 * convenient lookups: the source field bound to a column, the set of configured column families, the
 * columns within a family, and the families missing from a given set. It is used both to drive record
 * conversion and to validate the destination table.</p>
 *
 * @see com.gotocompany.depot.bigtable.client.BigTableClient
 * @see com.gotocompany.depot.bigtable.parser.BigTableRecordParser
 */
public class BigTableSchema {

    /** Parsed column-family mapping: family name to an object of column name to source field name. */
    private final JSONObject columnFamilyMapping;

    /**
     * Parses the column-family mapping from its JSON string form.
     *
     * @param columnMapping the column-family mapping as a JSON document; top-level keys are column
     *     families, each mapping column names to source field names
     * @throws com.gotocompany.depot.exception.ConfigurationException if {@code columnMapping} is
     *     {@code null} or empty
     * @throws org.json.JSONException if {@code columnMapping} is not valid JSON
     */
    public BigTableSchema(String columnMapping) {
        if (columnMapping == null || columnMapping.isEmpty()) {
            throw new ConfigurationException("Column Mapping should not be empty or null");
        }
        this.columnFamilyMapping = new JSONObject(columnMapping);
    }

    /**
     * Returns the source field name bound to a column within a column family.
     *
     * @param columnFamily the column family to look up
     * @param columnName the column within that family
     * @return the source field name configured for the given family and column
     * @throws org.json.JSONException if the column family or column is not present in the mapping
     */
    public String getField(String columnFamily, String columnName) {
        JSONObject columns = columnFamilyMapping.getJSONObject(columnFamily);
        return columns.getString(columnName);
    }

    /**
     * Returns the set of column families declared in the mapping.
     *
     * @return the configured column-family names
     */
    public Set<String> getColumnFamilies() {
        return columnFamilyMapping.keySet();
    }

    /**
     * Returns the set of columns configured within a column family.
     *
     * @param family the column family whose columns are requested
     * @return the column names mapped under {@code family}
     * @throws org.json.JSONException if {@code family} is not present in the mapping
     */
    public Set<String> getColumns(String family) {
        return columnFamilyMapping.getJSONObject(family).keySet();
    }

    /**
     * Returns missing column families.
     *
     * <p>Computes the set difference of the mapping's column families minus
     * {@code existingColumnFamilies}; the result is the families declared in the mapping but not
     * present in (for example) a Bigtable table. The input set is not modified.</p>
     *
     * @param existingColumnFamilies existing column families in a table.
     * @return set of missing column families
     */
    public Set<String> getMissingColumnFamilies(Set<String> existingColumnFamilies) {
        Set<String> tempSet = new HashSet<>(getColumnFamilies());
        tempSet.removeAll(existingColumnFamilies);
        return tempSet;
    }
}
