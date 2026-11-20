# Debezium Iceberg Converter (`debezium-iceberg-converter`)

The `debezium-iceberg-converter` module packages the Kafka Connect / Debezium `Schema` and `Struct` to Apache Iceberg `Schema`, `SortOrder`, and `GenericRecord` conversion engine as a **standalone, server-independent Java library**.

- **Minimal dependency footprint:** Depends only on `iceberg-core`, `debezium-connector-common`, `connect-api`, and `slf4j-api` (zero Quarkus, `debezium-server-core`, `debezium-embedded`, Hadoop, Hive, or Spark dependencies).
- **Single source of truth:** `debezium-server-iceberg-sink` delegates all Connect-format schema and record conversion to this module, guaranteeing identical type mapping, temporal precision, Iceberg v3 Variant, and geospatial (WKB/SRID) rules across Debezium Server, Debezium Embedded, and Kafka Connect.

For full architectural rationale, see [ADR-0002: Pluggable Iceberg Converter](adr/0002-iceberg-converter.md).

---

## Maven Coordinates

```xml
<dependency>
    <groupId>io.debezium</groupId>
    <artifactId>debezium-iceberg-converter</artifactId>
    <version>${version.debezium-server-iceberg}</version>
</dependency>
```

Artifacts are published to GitHub Packages (`https://maven.pkg.github.com/memiiso/debezium-server-iceberg`). When running Debezium Server Iceberg distribution (`debezium-server-iceberg-dist`), `debezium-iceberg-converter-*.jar` is already bundled in `lib/`.

---

## Two-Layer Usage

Because Debezium `3.7.x` and Kafka Connect's `Converter` SPI (`org.apache.kafka.connect.storage.Converter`) return `byte[]` and convert keys and values independently, the module provides two complementary layers:

| Layer | Class | Output | Key + Value Schema Together? | Best For |
| :--- | :--- | :--- | :--- | :--- |
| **Layer 1 (In-Memory)** | `io.debezium.converters.iceberg.IcebergStructConverter` | Live `ConvertedIcebergRecord` (`Schema`, `SortOrder`, `GenericRecord`) | **Yes** (`identifierFieldIds` & `SortOrder` preserved) | Debezium Embedded (`Connect.class`), `debezium-server-iceberg-sink`, custom JVM pipelines (zero serialization overhead) |
| **Layer 2 (Connect `Converter`)** | `io.debezium.converters.iceberg.IcebergConverter` | Serialized `byte[]` via pluggable `IcebergRecordSerializer` | Separate (`isKey=true` vs `isKey=false`) | Debezium Server (`clientprovided`), Debezium Embedded (`ClientProvided.class`), Kafka Connect |

---

## Layer 1: Zero-Copy In-Memory API (Debezium Embedded `Connect.class`)

Use `IcebergStructConverter` directly when consuming `SourceRecord` events in-process with Debezium Embedded (`ChangeEventFormat.of(Connect.class)`):

```java
import io.debezium.converters.iceberg.ConvertedIcebergRecord;
import io.debezium.converters.iceberg.IcebergConverterConfig;
import io.debezium.converters.iceberg.IcebergStructConverter;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.kafka.connect.source.SourceRecord;

IcebergConverterConfig config =
    IcebergConverterConfig.builder()
        .excludedColumns("internal_col")
        .geospatialTypesEnabled(true)
        .build();

IcebergStructConverter icebergConverter = new IcebergStructConverter(config);

DebeziumEngine<RecordChangeEvent<SourceRecord>> engine =
    DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
        .using(debeziumProps)
        .notifying(record -> {
            ConvertedIcebergRecord converted = icebergConverter.convert(record.record());
            if (converted.record() != null) {
                // converted.topic(), converted.schema(),
                // converted.sortOrder(), converted.record() (GenericRecord)
            }
        })
        .build();
```

`IcebergStructConverter` maintains a thread-safe bounded LRU cache (default `1024` entries, configurable via the `(config, maxSchemaCacheSize)` constructor) keyed by `(valueSchema, keySchema)`.

---

## Layer 2: Pluggable `IcebergConverter` (`Converter`)

`io.debezium.converters.iceberg.IcebergConverter` implements `org.apache.kafka.connect.storage.Converter`, `Versioned`, and `ConfigDescriptor`, and is registered via `META-INF/services/org.apache.kafka.connect.storage.Converter`.

### Debezium Server (`clientprovided`)

```properties
debezium.format.key=clientprovided
debezium.format.key.class=io.debezium.converters.iceberg.IcebergConverter

debezium.format.value=clientprovided
debezium.format.value.class=io.debezium.converters.iceberg.IcebergConverter
debezium.format.value.serializer.type=avro-envelope
debezium.format.value.excluded.columns=internal_col
```

!!! warning "Debezium `3.7.x` `clientprovided` Configuration Caveats"
    1. **Lowercase `clientprovided`:** `DebeziumSerializationProducer` checks `"clientprovided"` case-sensitively—always use lowercase.
    2. **Do not mix `format.key=connect` with `format.value=clientprovided`:** Debezium's `ConverterBuilder` only skips converters when the *value* format is `connect`. Pair `debezium.format.value=clientprovided` with a `Converter`-backed key format such as `clientprovided` or `json`.
    3. **Separate `key.converter.class` / `value.converter.class` when headers are enabled:** In Debezium Embedded (`KeyValueHeaderChangeEventFormat`), `ConverterBuilder.createHeaderConverter()` merges global `converter.*` properties into `header.converter.*`. If a header format (e.g., `Json.class`) is configured, set `key.converter.class` and `value.converter.class` rather than global `converter.class`. (Global `converter.class` works when no header format is configured.)

### Debezium Embedded (`ClientProvided.class`)

```java
Properties props = new Properties();
props.setProperty("key.converter.class", "io.debezium.converters.iceberg.IcebergConverter");
props.setProperty("value.converter.class", "io.debezium.converters.iceberg.IcebergConverter");
props.setProperty("value.converter.serializer.type", "avro-envelope");

AvroEnvelopeRecordSerializer serializer = new AvroEnvelopeRecordSerializer();

DebeziumEngine<ChangeEvent<Object, Object>> engine =
    DebeziumEngine.create(
            KeyValueHeaderChangeEventFormat.of(
                ClientProvided.class, ClientProvided.class, Json.class))
        .using(props)
        .notifying(event -> {
            byte[] valueBytes = (byte[]) event.value();
            if (valueBytes != null) {
                DeserializedIcebergRecord decoded = serializer.deserialize(valueBytes);
                // decoded.schema(), decoded.record() (GenericRecord)
            }
        })
        .build();
```

### Kafka Connect

```properties
value.converter=io.debezium.converters.iceberg.IcebergConverter
value.converter.serializer.type=avro-envelope
```

Consumers decode Iceberg payloads directly via `converter.deserializeIcebergRecord(bytes)` (or the matching `IcebergRecordSerializer.deserialize(bytes)`). Because `IcebergConverter` is strictly Iceberg-oriented, Kafka Connect reverse deserialization (`toConnectData`) throws `UnsupportedOperationException`.

---

## Wire Serializers (`serializer.type`)

Layer 2 delegates `byte[]` encoding/decoding to `io.debezium.converters.iceberg.spi.IcebergRecordSerializer`:

| `serializer.type` | Wire Format | Trade-offs |
| :--- | :--- | :--- |
| **`avro-envelope`** *(default)* | `[2B magic 0x49 0x43][1B version 0x01][4B schemaJsonLen][Iceberg SchemaParser JSON][Iceberg DataWriter Avro datum]` | **Zero external Schema Registry**; fully self-describing; preserves Iceberg field IDs, logical types, `Variant`, and v3 geospatial types. Repeats schema JSON per message. |
| **`avro-single-object`** | `AvroSingleObjectRecordSerializer` (Iceberg `IcebergEncoder` / `IcebergDecoder`): `[2B 0xC3 0x01][8B little-endian CRC-64-AVRO fingerprint][Avro datum]` | Minimal 10-byte per-record header for high-throughput Kafka pipelines. Cross-JVM consumers must resolve schemas via `registerSchema(Schema)` or an Avro `SchemaStore` (`avro.schema.store.class`); note that resolving through a plain Avro `SchemaStore` uses `AvroSchemaUtil.toIceberg` and cannot recover Iceberg-only metadata (`identifierFieldIds`, `Variant`, `Geometry`/`Geography` CRS). |
| **`<custom FQCN>`** | Custom class implementing `io.debezium.converters.iceberg.spi.IcebergRecordSerializer` (public no-arg constructor) | Full control over framing, compression, JSON, or custom Schema Registry integration. |

---

## Configuration Reference

All properties are exposed via `IcebergConverter.config()` (`ConfigDef`) and `IcebergConverter.getConfigFields()` (`ConfigDescriptor`):

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `serializer.type` | `STRING` | `avro-envelope` | Layer 2 wire codec: `avro-envelope`, `avro-single-object`, or custom `IcebergRecordSerializer` FQCN. |
| `avro.schema.store.class` | `STRING` | `""` | Optional `org.apache.avro.message.SchemaStore` FQCN (public no-arg constructor) used by `avro-single-object` to resolve unknown schema fingerprints across JVMs. |
| `temporal.precision.mode` | `STRING` | `isostring` | Fallback precision mode for untyped temporal values (`isostring`, `connect`, `microseconds`, `nanoseconds`; `adaptive` and `adaptive_time_microseconds` are also accepted for source compatibility). |
| `event.flattening.enabled` | `BOOLEAN` | `true` | Whether incoming Debezium events are flattened (e.g., via `ExtractNewRecordState` SMT). |
| `create.identifier.fields` | `BOOLEAN` | `true` | Whether primary key fields from `keySchema` are added as Iceberg identifier fields (Layer 1). |
| `preserve.required.property` | `BOOLEAN` | `false` | Preserve Kafka Connect field optionality instead of marking only identifier fields required. |
| `replace.null.with.default` | `BOOLEAN` | `false` | Replace `null` Struct field values with their Kafka Connect schema defaults. |
| `nested.as.variant` | `BOOLEAN` | `false` | Convert nested `STRUCT` fields to Iceberg v3 `VariantType`. |
| `excluded.columns` | `LIST` | `""` | Comma-separated top-level column names to omit from derived schemas and records. |
| `geospatial.types.enabled` | `BOOLEAN` | `false` | Map Debezium `Geometry` / `Geography` logical types to Iceberg v3 geospatial types. |
| `geometry.crs` | `STRING` | `OGC:CRS84` | CRS for Iceberg `Geometry` columns (`OGC:CRS84`, `EPSG:<N>`, or `srid:0`). |
| `geospatial.allow.missing.srid` | `BOOLEAN` | `false` | Allow geospatial values without an SRID (`null` or `0`) to use the column CRS instead of failing. |
| `schema.cache.size` | `INT` | `1024` | Maximum number of cached `(valueSchema, keySchema)` derivations (`0` disables caching). |
