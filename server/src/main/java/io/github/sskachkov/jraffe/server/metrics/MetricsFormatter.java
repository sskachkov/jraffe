package io.github.sskachkov.jraffe.server.metrics;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

public final class MetricsFormatter {
    private MetricsFormatter() {}

    public static List<String> format(MeterRegistry registry) {
        List<Meter> meters = new ArrayList<>(registry.getMeters());
        meters.sort(Comparator.comparing(meter -> formatTags(meter) + meter.getId().getName()));

        List<String> lines = new ArrayList<>();
        for (Meter meter : meters) {
            String name = meter.getId().getName();
            if (name.endsWith(".percentile")) {
                continue;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(formatTags(meter)).append(" ").append(name).append(":");
            if (meter instanceof Timer timer) {
                ValueAtPercentile[] percentiles = timer.takeSnapshot().percentileValues();
                if (percentiles.length > 0) {
                    StringJoiner joiner = new StringJoiner(" ", "[", "]");
                    for (ValueAtPercentile p : percentiles) {
                        joiner.add("p" + (int) (p.percentile() * 100) + "=" + String.format(Locale.US, "%.2f", p.value(TimeUnit.MILLISECONDS)));
                    }
                    sb.append(" ").append(joiner);
                } else {
                    sb.append(" [mean=").append(String.format(Locale.US, "%.2f", timer.mean(TimeUnit.MILLISECONDS)));
                    sb.append(" max=").append(String.format(Locale.US, "%.2f", timer.max(TimeUnit.MILLISECONDS))).append("]");
                }
                sb.append(" [count=").append(timer.count()).append("]");
            } else {
                for (Measurement measure : meter.measure()) {
                    sb.append(" [").append(measure.getStatistic()).append(" = ").append(measure.getValue()).append("]");
                }
            }
            lines.add(sb.toString());
        }
        return lines;
    }

    private static String formatTags(Meter meter) {
        List<Tag> tags = meter.getId().getTags();
        if (tags.isEmpty()) {
            return "";
        }
        StringJoiner joiner = new StringJoiner(",", "{", "}");
        for (Tag tag : tags) {
            joiner.add(tag.getKey() + "=" + tag.getValue());
        }
        return joiner.toString();
    }
}
