package com.analyticsplatform.bench.report;

import com.analyticsplatform.bench.report.BenchmarkReport.ConfigSummary;
import com.analyticsplatform.bench.run.BenchmarkStore.Environment;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Renders a benchmark report as markdown.
 *
 * <p>Writes the environment and the refusal reasons as prominently as the headline. A report that
 * leads with a percentage and buries "the correctness gate failed" in a footnote is worse than no
 * report — it is a number someone will quote.
 */
public final class MarkdownReportWriter {

    private MarkdownReportWriter() {
    }

    public static void write(
            Path target, BenchmarkReport report, Environment environment,
            List<String> ablationOrder) {

        StringBuilder out = new StringBuilder();
        out.append("# Benchmark results\n\n");
        out.append("_Generated ").append(Instant.now()).append("._\n\n");

        writeHeadline(out, report);
        writeEnvironment(out, environment);
        writeConfigurations(out, report);
        writeAblation(out, report, ablationOrder);
        writeCaveats(out);

        try {
            Files.createDirectories(target.getParent());
            Files.writeString(target, out.toString());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write " + target, e);
        }
    }

    private static void writeHeadline(StringBuilder out, BenchmarkReport report) {
        out.append("## Result\n\n");

        if (!report.isValid()) {
            // Leads with the refusal. Anyone skimming for a number must hit this first.
            out.append("**No headline figure: this measurement is not admissible.**\n\n");
            for (String reason : report.invalidReasons()) {
                out.append("- ").append(reason).append('\n');
            }
            out.append("\nA percentage derived from these runs would not mean anything, ")
                    .append("so none is reported.\n\n");
            return;
        }

        report.headline().ifPresentOrElse(headline -> {
            out.append("> ").append(headline.claim()).append("\n\n");
            out.append("| | median | mean | min | max | sd | n |\n");
            out.append("|---|---|---|---|---|---|---|\n");
            appendStatsRow(out, headline.baselineLabel(), headline.baseline());
            appendStatsRow(out, headline.optimizedLabel(), headline.optimized());
            out.append('\n');

            if (!headline.optimized().isStatisticallyMeaningful()) {
                out.append("**Caution:** fewer than 5 measured iterations. ")
                        .append("The figure is reported as measured, but the sample is small ")
                        .append("enough that it should not be treated as precise.\n\n");
            }
            if (headline.improvementFraction() < 0) {
                out.append("**This is a regression, not an improvement.** ")
                        .append("Reported as measured rather than suppressed.\n\n");
            }
        }, () -> out.append("No headline: the baseline or optimized configuration was not run.\n\n"));
    }

    private static void appendStatsRow(StringBuilder out, String label, BenchmarkStatistics stats) {
        out.append(String.format("| %s | %.0fms | %.0fms | %dms | %dms | %.1fms | %d |%n",
                label, stats.medianMillis(), stats.meanMillis(),
                stats.minMillis(), stats.maxMillis(), stats.stddevMillis(),
                stats.observations()));
    }

    private static void writeEnvironment(StringBuilder out, Environment environment) {
        out.append("## Environment\n\n");
        out.append("A measurement without its conditions cannot be reproduced or compared ")
                .append("against a later one.\n\n");
        out.append("| | |\n|---|---|\n");
        row(out, "Spark", environment.sparkVersion());
        row(out, "Java", environment.javaVersion());
        row(out, "ClickHouse", environment.clickhouseVersion());
        row(out, "Cores", String.valueOf(environment.totalCores()));
        row(out, "Executor memory", environment.executorMemory());
        row(out, "Executor overhead", environment.executorOverhead());
        row(out, "Git commit", environment.gitCommit());
        row(out, "Cache policy", environment.cachePolicy());
        out.append('\n');
    }

    private static void row(StringBuilder out, String key, String value) {
        out.append("| ").append(key).append(" | ")
                .append(value == null ? "_not captured_" : value).append(" |\n");
    }

    private static void writeConfigurations(StringBuilder out, BenchmarkReport report) {
        out.append("## All configurations\n\n");
        out.append("Every configuration is listed, not only the pair that produces the ")
                .append("largest gap.\n\n");
        out.append("| config | median | mean | sd | cv | n |\n|---|---|---|---|---|---|\n");

        report.summaries().values().stream()
                .sorted(java.util.Comparator.comparing(s -> s.config().label()))
                .forEach(summary -> {
                    BenchmarkStatistics stats = summary.statistics();
                    out.append(String.format("| `%s` | %.0fms | %.0fms | %.1fms | %.1f%% | %d |%n",
                            summary.config().label(), stats.medianMillis(), stats.meanMillis(),
                            stats.stddevMillis(), stats.coefficientOfVariation() * 100,
                            stats.observations()));
                });
        out.append('\n');
    }

    private static void writeAblation(
            StringBuilder out, BenchmarkReport report, List<String> ablationOrder) {

        if (ablationOrder == null || ablationOrder.isEmpty()) {
            return;
        }
        Map<String, Double> deltas = report.ablationDeltas(ablationOrder);
        if (deltas.isEmpty()) {
            return;
        }

        out.append("## Ablation\n\n");
        out.append("Marginal contribution of each step over the one before it. ")
                .append("A single headline percentage says the tuning worked; this says ")
                .append("which part of it did.\n\n");
        out.append("| step | marginal change |\n|---|---|\n");
        deltas.forEach((label, delta) ->
                out.append(String.format("| `%s` | %+.1f%% |%n", label, delta * 100)));

        out.append("\nCumulative order attributes credit in one arbitrary sequence. ")
                .append("The leave-one-out rows (`L*`) bracket it from the other side.\n\n");
    }

    private static void writeCaveats(StringBuilder out) {
        out.append("## How to read this\n\n");
        out.append("- Improvement is computed from **medians**, not means. ")
                .append("Docker on a laptop produces occasional outliers, and a median ")
                .append("keeps one disturbed run from moving the figure.\n");
        out.append("- The headline **names its baseline**. ")
                .append("\"Improved Spark performance by X%\" would invite the reader to ")
                .append("assume the largest available gap.\n");
        out.append("- Configurations are run in **alternating order** with warm-ups excluded, ")
                .append("so cache and JIT drift is not attributed to whichever ran first.\n");
        out.append("- Every measured run passed a **correctness gate** comparing its output ")
                .append("against the baseline's. A configuration that computed something else ")
                .append("is discarded rather than reported as fast.\n");
        out.append("- Compression appears only in Experiment B. ")
                .append("Mixing a physical-layout change into an execution measurement would ")
                .append("make the number unattributable to either.\n");
        out.append("- Postgres, ClickHouse and Kafka share the same Docker VM as Spark. ")
                .append("Memory contention is real and is why the spread is reported ")
                .append("alongside the median.\n");
    }
}
