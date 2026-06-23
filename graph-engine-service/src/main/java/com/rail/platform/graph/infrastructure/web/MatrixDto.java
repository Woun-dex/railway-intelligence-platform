package com.rail.platform.graph.infrastructure.web;

import java.util.List;

/**
 * JSON view of the rail network for the matrix endpoint and the viewer.
 *
 * @param graphVersion current snapshot version
 * @param stationCount number of stations
 * @param stations     station metadata (indexed by {@code index})
 * @param edges        directed edges as {@code [fromIndex, toIndex, weightSeconds]}
 * @param order        display permutation: {@code order[displayPos] = stationIndex},
 *                     a line-clustered seriation so the matrix shows block structure
 */
public record MatrixDto(String graphVersion, int stationCount,
                        List<Station> stations, List<int[]> edges, int[] order) {

    public record Station(int index, String id, String name, double lat, double lon,
                          List<String> lines, double pagerank) {
    }
}
