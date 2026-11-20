/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import io.debezium.config.Field;
import io.debezium.converters.iceberg.geospatial.GeospatialTypes;
import io.debezium.converters.iceberg.serde.IcebergRecordSerializers;
import io.debezium.jdbc.TemporalPrecisionMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

/**
 * Kafka Connect {@link ConverterConfig}-backed implementation of {@link IcebergConverterConfig}.
 *
 * <p>Constructed directly by Kafka Connect / Debezium {@code Converter} instances (with {@link
 * ConverterConfig#TYPE_CONFIG} set) or programmatically via {@link #fromMap(Map)} and {@link
 * #defaults()}.
 */
public class ConnectIcebergConverterConfig extends ConverterConfig
    implements IcebergConverterConfig {

  public static final Field TEMPORAL_PRECISION_MODE =
      Field.create("temporal.precision.mode")
          .withDisplayName("Temporal precision mode")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDefault(TemporalPrecisionMode.ISOSTRING.getValue())
          .withDescription(
              "Fallback precision mode for untyped temporal values (e.g., isostring, connect,"
                  + " microseconds, nanoseconds).");

  public static final Field EVENT_FLATTENING_ENABLED =
      Field.create("event.flattening.enabled")
          .withDisplayName("Event flattening enabled")
          .withType(ConfigDef.Type.BOOLEAN)
          .withImportance(ConfigDef.Importance.HIGH)
          .withDefault(true)
          .withDescription(
              "Whether incoming Debezium events are flattened (e.g., via ExtractNewRecordState"
                  + " SMT).");

  public static final Field CREATE_IDENTIFIER_FIELDS =
      Field.create("create.identifier.fields")
          .withDisplayName("Create identifier fields")
          .withType(ConfigDef.Type.BOOLEAN)
          .withImportance(ConfigDef.Importance.HIGH)
          .withDefault(true)
          .withDescription(
              "Whether primary key fields from the key schema should be added as Iceberg"
                  + " identifier fields.");

  public static final Field PRESERVE_REQUIRED_PROPERTY =
      Field.create("preserve.required.property")
          .withDisplayName("Preserve required property")
          .withType(ConfigDef.Type.BOOLEAN)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDefault(false)
          .withDescription(
              "Whether to preserve Kafka Connect field optionality in the derived Iceberg schema"
                  + " rather than marking only identifier fields required.");

  public static final Field REPLACE_NULL_WITH_DEFAULT =
      Field.create("replace.null.with.default")
          .withDisplayName("Replace null with default")
          .withType(ConfigDef.Type.BOOLEAN)
          .withImportance(ConfigDef.Importance.LOW)
          .withDefault(false)
          .withDescription(
              "Whether null Struct field values should be replaced with their Kafka Connect schema"
                  + " default values.");

  public static final Field NESTED_AS_VARIANT =
      Field.create("nested.as.variant")
          .withDisplayName("Nested structs as Variant")
          .withType(ConfigDef.Type.BOOLEAN)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDefault(false)
          .withDescription(
              "Whether nested Struct fields should be converted to Iceberg v3 VariantType.");

  public static final Field EXCLUDED_COLUMNS =
      Field.create("excluded.columns")
          .withDisplayName("Excluded columns")
          .withType(ConfigDef.Type.LIST)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDefault("")
          .withDescription(
              "Comma-separated list of top-level column names to exclude from the Iceberg schema"
                  + " and converted records.");

  public static final Field GEOSPATIAL_TYPES_ENABLED =
      Field.create("geospatial.types.enabled")
          .withDisplayName("Enable Iceberg geospatial types")
          .withType(ConfigDef.Type.BOOLEAN)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDefault(false)
          .withDescription(
              "Whether Debezium Geometry and Geography logical types should be mapped to Iceberg"
                  + " v3 geospatial types.");

  public static final Field GEOMETRY_CRS =
      Field.create("geometry.crs")
          .withDisplayName("Geometry CRS")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDefault(Types.GeometryType.DEFAULT_CRS)
          .withDescription(
              "Coordinate Reference System for Iceberg Geometry columns (OGC:CRS84, EPSG:<N>, or"
                  + " srid:0).");

  public static final Field GEOSPATIAL_ALLOW_MISSING_SRID =
      Field.create("geospatial.allow.missing.srid")
          .withDisplayName("Allow missing SRID for geospatial values")
          .withType(ConfigDef.Type.BOOLEAN)
          .withImportance(ConfigDef.Importance.LOW)
          .withDefault(false)
          .withDescription(
              "Whether geospatial values without an SRID (null or 0) should be written using the"
                  + " column CRS instead of failing.");

  public static final Field SCHEMA_CACHE_SIZE =
      Field.create("schema.cache.size")
          .withDisplayName("Schema cache size")
          .withType(ConfigDef.Type.INT)
          .withImportance(ConfigDef.Importance.LOW)
          .withDefault(IcebergStructConverter.DEFAULT_SCHEMA_CACHE_SIZE)
          .withDescription(
              "Maximum number of derived Iceberg schemas cached by Connect (valueSchema, keySchema)"
                  + " pair. Set to 0 to disable caching.");

  public static final Field SERIALIZER_TYPE =
      Field.create("serializer.type")
          .withDisplayName("Iceberg record serializer type")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.HIGH)
          .withDefault(IcebergRecordSerializers.DEFAULT_TYPE)
          .withDescription(
              "Wire serializer for Layer 2 IcebergConverter: 'avro-envelope' (default),"
                  + " 'avro-single-object', or a custom IcebergRecordSerializer FQCN.");

  public static final Field AVRO_SCHEMA_STORE_CLASS =
      Field.create("avro.schema.store.class")
          .withDisplayName("Avro SchemaStore class")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.LOW)
          .withDefault("")
          .withDescription(
              "Optional FQCN implementing org.apache.avro.message.SchemaStore (public no-arg"
                  + " constructor) used by 'avro-single-object' to resolve unknown schema"
                  + " fingerprints across JVMs.");

  private static final ConfigDef.Validator TEMPORAL_PRECISION_MODE_VALIDATOR =
      (name, value) -> {
        if (value == null || TemporalPrecisionMode.parse(value.toString()) == null) {
          throw new ConfigException(name, value, "Invalid temporal.precision.mode value: " + value);
        }
      };

  private static final Field.Set ALL_FIELDS =
      Field.setOf(
          TEMPORAL_PRECISION_MODE,
          EVENT_FLATTENING_ENABLED,
          CREATE_IDENTIFIER_FIELDS,
          PRESERVE_REQUIRED_PROPERTY,
          REPLACE_NULL_WITH_DEFAULT,
          NESTED_AS_VARIANT,
          EXCLUDED_COLUMNS,
          GEOSPATIAL_TYPES_ENABLED,
          GEOMETRY_CRS,
          GEOSPATIAL_ALLOW_MISSING_SRID,
          SCHEMA_CACHE_SIZE,
          SERIALIZER_TYPE,
          AVRO_SCHEMA_STORE_CLASS);

  private static final Map<String, ConfigDef.Validator> VALIDATORS =
      Map.of(
          TEMPORAL_PRECISION_MODE.name(),
          TEMPORAL_PRECISION_MODE_VALIDATOR,
          SCHEMA_CACHE_SIZE.name(),
          ConfigDef.Range.atLeast(0));

  private static final ConfigDef CONFIG = buildConfigDef();

  private static ConfigDef buildConfigDef() {
    ConfigDef configDef = ConverterConfig.newConfigDef();
    for (Field field : ALL_FIELDS) {
      Object defaultVal =
          field.type() == ConfigDef.Type.LIST ? Collections.emptyList() : field.defaultValue();
      configDef.define(
          field.name(),
          field.type(),
          defaultVal,
          VALIDATORS.get(field.name()),
          field.importance(),
          field.description());
    }
    return configDef;
  }

  private final TemporalPrecisionMode temporalPrecisionMode;
  private final List<String> excludedColumns;

  public ConnectIcebergConverterConfig(Map<String, ?> props) {
    super(CONFIG, props);
    this.temporalPrecisionMode =
        TemporalPrecisionMode.parse(getString(TEMPORAL_PRECISION_MODE.name()));
    List<String> rawExcluded = getList(EXCLUDED_COLUMNS.name());
    this.excludedColumns =
        rawExcluded == null
            ? Collections.emptyList()
            : rawExcluded.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    GeospatialTypes.validateConfig(this);
  }

  public static ConfigDef configDef() {
    return CONFIG;
  }

  public static Field.Set allFields() {
    return ALL_FIELDS;
  }

  /**
   * Creates a {@link ConnectIcebergConverterConfig} from a plain map, defaulting {@code
   * converter.type=value} when not explicitly supplied.
   */
  public static ConnectIcebergConverterConfig fromMap(Map<String, ?> props) {
    Objects.requireNonNull(props, "props must not be null");
    Map<String, Object> copy = new HashMap<>(props);
    copy.putIfAbsent(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
    return new ConnectIcebergConverterConfig(copy);
  }

  public static ConnectIcebergConverterConfig defaults() {
    return fromMap(Collections.emptyMap());
  }

  @Override
  public TemporalPrecisionMode temporalPrecisionMode() {
    return temporalPrecisionMode;
  }

  @Override
  public boolean eventFlatteningEnabled() {
    return getBoolean(EVENT_FLATTENING_ENABLED.name());
  }

  @Override
  public boolean createIdentifierFields() {
    return getBoolean(CREATE_IDENTIFIER_FIELDS.name());
  }

  @Override
  public boolean preserveRequiredProperty() {
    return getBoolean(PRESERVE_REQUIRED_PROPERTY.name());
  }

  @Override
  public boolean replaceNullWithDefault() {
    return getBoolean(REPLACE_NULL_WITH_DEFAULT.name());
  }

  @Override
  public boolean nestedAsVariant() {
    return getBoolean(NESTED_AS_VARIANT.name());
  }

  @Override
  public List<String> excludedColumns() {
    return excludedColumns;
  }

  @Override
  public boolean geospatialTypesEnabled() {
    return getBoolean(GEOSPATIAL_TYPES_ENABLED.name());
  }

  @Override
  public String geometryCrs() {
    return getString(GEOMETRY_CRS.name());
  }

  @Override
  public boolean geospatialAllowMissingSrid() {
    return getBoolean(GEOSPATIAL_ALLOW_MISSING_SRID.name());
  }

  public int schemaCacheSize() {
    return getInt(SCHEMA_CACHE_SIZE.name());
  }

  public String serializerType() {
    return getString(SERIALIZER_TYPE.name());
  }

  public String avroSchemaStoreClass() {
    return getString(AVRO_SCHEMA_STORE_CLASS.name());
  }
}
