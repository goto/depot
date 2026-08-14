package com.gotocompany.depot.config;

import org.aeonbits.owner.Config;

/**
 * Owner configuration interface for the Google Cloud Bigtable sink.
 *
 * <p>{@code BigTableSinkConfig} supplies the coordinates and write settings Depot needs to connect to
 * a Bigtable instance and map records onto it: the target project, instance, and table, the
 * credentials to authenticate with, and the templates that derive each row key and column-family
 * placement. It extends {@link SinkConfig} so the shared sink and schema settings are also available.
 * The {@code @Config.DisableFeature(PARAMETER_FORMATTING)} annotation disables Owner's parameter
 * expansion so that property values are used literally.
 */
@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
public interface BigTableSinkConfig extends SinkConfig {
    /**
     * Returns the Google Cloud project ID that owns the target Bigtable instance.
     *
     * <p>Bound to the {@code SINK_BIGTABLE_GOOGLE_CLOUD_PROJECT_ID} property; has no default.
     *
     * @return the Google Cloud project ID
     */
    @Key("SINK_BIGTABLE_GOOGLE_CLOUD_PROJECT_ID")
    String getGCloudProjectID();

    /**
     * Returns the identifier of the Bigtable instance to write to.
     *
     * <p>Bound to the {@code SINK_BIGTABLE_INSTANCE_ID} property; has no default.
     *
     * @return the Bigtable instance ID
     */
    @Key("SINK_BIGTABLE_INSTANCE_ID")
    String getInstanceId();

    /**
     * Returns the name of the Bigtable table that records are written to.
     *
     * <p>Bound to the {@code SINK_BIGTABLE_TABLE_ID} property; has no default.
     *
     * @return the Bigtable table ID
     */
    @Key("SINK_BIGTABLE_TABLE_ID")
    String getTableId();

    /**
     * Returns the filesystem path to the Google Cloud service-account credentials JSON file.
     *
     * <p>The credentials authenticate Depot with Bigtable. Bound to the
     * {@code SINK_BIGTABLE_CREDENTIAL_PATH} property; has no default.
     *
     * @return the path to the service-account credential file
     */
    @Key("SINK_BIGTABLE_CREDENTIAL_PATH")
    String getCredentialPath();

    /**
     * Returns the template used to construct the Bigtable row key for each record.
     *
     * <p>The template is rendered against message fields (see
     * {@link com.gotocompany.depot.common.Template}) to produce the row key under which a record is
     * stored. Bound to the {@code SINK_BIGTABLE_ROW_KEY_TEMPLATE} property; has no default.
     *
     * @return the row-key template expression
     */
    @Key("SINK_BIGTABLE_ROW_KEY_TEMPLATE")
    String getRowKeyTemplate();

    /**
     * Returns the mapping that assigns record fields to Bigtable column families.
     *
     * <p>The value describes which column family each field is written under; an empty or absent
     * mapping is rejected at startup with a
     * {@link com.gotocompany.depot.exception.ConfigurationException}. Bound to the
     * {@code SINK_BIGTABLE_COLUMN_FAMILY_MAPPING} property; has no default.
     *
     * @return the raw column-family mapping specification
     */
    @Key("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING")
    String getColumnFamilyMapping();
}
