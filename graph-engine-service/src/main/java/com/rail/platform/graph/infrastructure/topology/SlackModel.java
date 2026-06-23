package com.rail.platform.graph.infrastructure.topology;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Derives the recovery margins (slack) of the event-activity network from the
 * timetable, since GTFS carries no explicit slack.
 *
 * <p>The grounding assumption is that the <em>fastest</em> observed traversal of
 * a segment (or the shortest observed dwell) is close to the technical minimum,
 * so the margin a normal run carries is {@code typical − minimum}. A delay is
 * absorbed up to that margin before it propagates.
 */
@Component
public class SlackModel {

    /** Upper bound on per-edge running slack (defends against feed outliers). */
    private final int runSlackCapSec;
    /** Upper bound on per-station dwell slack. */
    private final int dwellSlackCapSec;
    /** Assumed scheduled connection buffer on top of the minimum transfer time. */
    private final int transferBufferSec;

    public SlackModel(
            @Value("${rail.graph.slack.run-cap-sec:600}") int runSlackCapSec,
            @Value("${rail.graph.slack.dwell-cap-sec:180}") int dwellSlackCapSec,
            @Value("${rail.graph.slack.transfer-buffer-sec:120}") int transferBufferSec) {
        this.runSlackCapSec = runSlackCapSec;
        this.dwellSlackCapSec = dwellSlackCapSec;
        this.transferBufferSec = transferBufferSec;
    }

    /** Running recovery margin = mean − minimum observed run time on the segment. */
    public int runSlack(int minRunSec, long sumRunSec, int count) {
        if (count <= 0) {
            return 0;
        }
        double mean = (double) sumRunSec / count;
        return clamp((int) Math.round(mean - minRunSec), runSlackCapSec);
    }

    /** Dwell recovery margin = mean − minimum observed dwell at the station. */
    public int dwellSlack(int minDwellSec, long sumDwellSec, int count) {
        if (count <= 0) {
            return 0;
        }
        double mean = (double) sumDwellSec / count;
        return clamp((int) Math.round(mean - minDwellSec), dwellSlackCapSec);
    }

    /** Connection slack a passenger has beyond the minimum transfer time. */
    public int transferSlack(int minTransferSec) {
        return Math.max(0, transferBufferSec);
    }

    private static int clamp(int v, int cap) {
        if (v < 0) {
            return 0;
        }
        return Math.min(v, cap);
    }
}
