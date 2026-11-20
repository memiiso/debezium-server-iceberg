/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg.serde;

import io.debezium.converters.iceberg.ConnectIcebergConverterConfig;
import io.debezium.converters.iceberg.IcebergStructConverter;
import io.debezium.converters.iceberg.spi.IcebergRecordSerializer;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.message.SchemaStore;
import org.apache.kafka.common.config.ConfigException;

/**
 * Factory resolving built-in or custom FQCN {@link IcebergRecordSerializer}
 * implementations.
 */
public final class IcebergRecordSerializers {

  public static final String DEFAULT_TYPE = AvroEnvelopeRecordSerializer.TYPE;

  private IcebergRecordSerializers() {
  }

  /**
   * Creates and configures an {@link IcebergRecordSerializer} for the given
   * {@code serializerType}
   * ({@code avro-envelope}, {@code avro-single-object}, or a fully-qualified
   * class name
   * implementing {@link IcebergRecordSerializer}).
   */
  public static IcebergRecordSerializer create(String serializerType, Map<String, ?> configs) {
    Objects.requireNonNull(configs, "configs must not be null");
    if (serializerType == null || serializerType.isBlank()) {
      throw new ConfigException(
          ConnectIcebergConverterConfig.SERIALIZER_TYPE.name(),
          serializerType,
          "serializer.type must not be null or blank");
    }
    String trimmed = serializerType.trim();

    IcebergRecordSerializer serializer = switch (trimmed.toLowerCase(Locale.ROOT)) {
      case AvroEnvelopeRecordSerializer.TYPE -> new AvroEnvelopeRecordSerializer();
      case AvroSingleObjectRecordSerializer.TYPE -> new AvroSingleObjectRecordSerializer();
      default -> instantiateCustom(trimmed);
    };

    serializer.configure(configs);
    return serializer;
  }

  static int schemaCacheSize(Map<String, ?> configs) {
    Objects.requireNonNull(configs, "configs must not be null");
    Object raw = configs.get(ConnectIcebergConverterConfig.SCHEMA_CACHE_SIZE.name());
    if (raw == null) {
      return IcebergStructConverter.DEFAULT_SCHEMA_CACHE_SIZE;
    }
    int parsed = raw instanceof Number n ? n.intValue() : Integer.parseInt(raw.toString().trim());
    if (parsed < 0) {
      throw new IllegalArgumentException(
          ConnectIcebergConverterConfig.SCHEMA_CACHE_SIZE.name() + " must be >= 0, got: " + parsed);
    }
    return parsed;
  }

  static SchemaStore instantiateSchemaStore(Map<String, ?> configs) {
    Objects.requireNonNull(configs, "configs must not be null");
    Object raw = configs.get(ConnectIcebergConverterConfig.AVRO_SCHEMA_STORE_CLASS.name());
    if (raw == null) {
      return null;
    }
    String className = raw.toString().trim();
    if (className.isEmpty()) {
      return null;
    }
    return instantiateByClassName(
        ConnectIcebergConverterConfig.AVRO_SCHEMA_STORE_CLASS.name(), className, SchemaStore.class);
  }

  private static IcebergRecordSerializer instantiateCustom(String className) {
    return instantiateByClassName(
        ConnectIcebergConverterConfig.SERIALIZER_TYPE.name(),
        className,
        IcebergRecordSerializer.class);
  }

  private static <T> T instantiateByClassName(
      String configName, String className, Class<T> expectedType) {
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      Class<?> clazz;
      try {
        clazz =
            Class.forName(
                className, true, cl != null ? cl : IcebergRecordSerializers.class.getClassLoader());
      } catch (ClassNotFoundException e) {
        clazz = Class.forName(className);
      }
      if (!expectedType.isAssignableFrom(clazz)) {
        throw new ConfigException(
            configName, className, "Class does not implement " + expectedType.getName());
      }
      return expectedType.cast(clazz.getDeclaredConstructor().newInstance());
    } catch (ConfigException e) {
      throw e;
    } catch (ReflectiveOperationException e) {
      throw new ConfigException(
          configName,
          className,
          "Failed to instantiate "
              + expectedType.getSimpleName()
              + " '"
              + className
              + "': "
              + e.getMessage());
    }
  }
}
