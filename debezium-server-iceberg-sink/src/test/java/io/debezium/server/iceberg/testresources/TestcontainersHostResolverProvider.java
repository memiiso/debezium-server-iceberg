/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.stream.Stream;

/**
 * Resolves {@code host.testcontainers.internal} to the loopback address in the test JVM, all the
 * other names use the built-in resolver.
 *
 * <p>Containers reach host ports exposed with {@code Testcontainers.exposeHostPorts} through this
 * name. The REST catalogs return the S3 url they use to the clients, so the test JVM must resolve
 * the same name, see {@link S3Minio#getS3UrlFromContainers()}. Registered in {@code
 * META-INF/services/java.net.spi.InetAddressResolverProvider}.
 */
public class TestcontainersHostResolverProvider extends InetAddressResolverProvider {

  // GenericContainer.INTERNAL_HOST_HOSTNAME, not referenced to keep the resolver free of
  // testcontainers classes, it is loaded on the first name lookup of the JVM
  static final String INTERNAL_HOST_HOSTNAME = "host.testcontainers.internal";

  @Override
  public InetAddressResolver get(Configuration configuration) {
    InetAddressResolver builtin = configuration.builtinResolver();
    return new InetAddressResolver() {
      @Override
      public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy)
          throws UnknownHostException {
        if (INTERNAL_HOST_HOSTNAME.equalsIgnoreCase(host)) {
          return Stream.of(InetAddress.getByAddress(host, new byte[] {127, 0, 0, 1}));
        }
        return builtin.lookupByName(host, lookupPolicy);
      }

      @Override
      public String lookupByAddress(byte[] addr) throws UnknownHostException {
        return builtin.lookupByAddress(addr);
      }
    };
  }

  @Override
  public String name() {
    return "testcontainers-host-resolver";
  }
}
