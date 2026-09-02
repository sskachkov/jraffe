package io.github.sskachkov.jraffe.server.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsFormatterTest {

    @Test
    void emptyRegistryProducesNoLines() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        assertEquals(List.of(), MetricsFormatter.format(registry));
    }

    @Test
    void counterIsFormattedWithNameAndValue() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.counter("requests.total").increment(3);

        List<String> lines = MetricsFormatter.format(registry);
        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("requests.total"));
        assertTrue(lines.get(0).contains("3.0"));
    }

    @Test
    void tagsAreIncludedInOutput() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.counter("requests.total", "peer", "n2").increment();

        String line = MetricsFormatter.format(registry).get(0);
        assertTrue(line.contains("{peer=n2}"), line);
    }

    @Test
    void timerWithoutPercentilesFallsBackToMeanAndMax() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.timer("rpc.latency").record(java.time.Duration.ofMillis(10));

        String line = MetricsFormatter.format(registry).get(0);
        assertTrue(line.contains("mean="), line);
        assertTrue(line.contains("max="), line);
        assertTrue(line.contains("[count=1]"), line);
    }

    @Test
    void linesAreSortedByTagsThenName() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.counter("b.metric").increment();
        registry.counter("a.metric").increment();

        List<String> lines = MetricsFormatter.format(registry);
        assertEquals(2, lines.size());
        assertTrue(lines.get(0).contains("a.metric"));
        assertTrue(lines.get(1).contains("b.metric"));
    }
}
