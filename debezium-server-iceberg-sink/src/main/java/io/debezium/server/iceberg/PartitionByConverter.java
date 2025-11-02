package io.debezium.server.iceberg;

import org.eclipse.microprofile.config.spi.Converter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Custom converter for partition-by configuration that supports custom delimiter.
 * Handles comma-separated values while respecting parentheses (e.g., "bucket(10, id), year(ts)").
 */
public class PartitionByConverter implements Converter<List<String>> {

    private static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";

    /**
     * Converts a partition-by configuration string into a list of partition spec strings.
     * <p>
     * This method supports custom delimiters and correctly handles comma-separated values,
     * including those containing parentheses (e.g., "bucket(10, id), year(ts)").
     * It splits the input string on commas that are not inside parentheses, trims whitespace,
     * and filters out empty segments.
     * <ul>
     *   <li>If the input is null or empty, returns an empty list.</li>
     *   <li>Example input: "bucket(10, id), year(ts)" → ["bucket(10, id)", "year(ts)"]</li>
     *   <li>Example input: "id, ts" → ["id", "ts"]</li>
     * </ul>
     *
     * See the [Iceberg documentation](#https://iceberg.apache.org/docs/latest/partitioning/) for more details on partition transforms.
     *
     * @param value the partition-by configuration string
     * @return a list of partition spec strings
     * @throws IllegalArgumentException if the input is invalid
     * @throws NullPointerException if the input is null
     */
    @Override
    public List<String> convert(String value) throws IllegalArgumentException, NullPointerException {
        if (value == null || value.trim().isEmpty()) {
            return List.of();
        }

        return Arrays.stream(value.split(COMMA_NO_PARENS_REGEX))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}
