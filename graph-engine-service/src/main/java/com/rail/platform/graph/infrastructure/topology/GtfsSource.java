package com.rail.platform.graph.infrastructure.topology;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

/**
 * Opens GTFS member files ({@code stops.txt}, {@code stop_times.txt}, ...) from
 * either a directory (filesystem or classpath, via Spring {@link Resource}) or a
 * {@code .zip} feed. This is the only place that knows where the bytes live.
 */
interface GtfsSource extends Closeable {

    boolean has(String fileName);

    /** Opens the named member, or {@code null} if it is absent. */
    InputStream open(String fileName) throws IOException;

    /**
     * Resolves a location string to a source: a path ending in {@code .zip} is
     * read as an archive, anything else as a directory base ({@code classpath:},
     * {@code file:} and bare paths all work).
     */
    static GtfsSource resolve(String location, ResourceLoader resourceLoader) throws IOException {
        if (location.toLowerCase().endsWith(".zip")) {
            Resource res = resourceLoader.getResource(location);
            return new ZipGtfsSource(new ZipFile(res.getFile()));
        }
        return new DirGtfsSource(resourceLoader, location);
    }

    /** Directory / classpath source. */
    final class DirGtfsSource implements GtfsSource {
        private final ResourceLoader resourceLoader;
        private final String base;

        DirGtfsSource(ResourceLoader resourceLoader, String base) {
            this.resourceLoader = resourceLoader;
            this.base = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        }

        @Override
        public boolean has(String fileName) {
            return resourceLoader.getResource(base + "/" + fileName).exists();
        }

        @Override
        public InputStream open(String fileName) throws IOException {
            Resource res = resourceLoader.getResource(base + "/" + fileName);
            return res.exists() ? res.getInputStream() : null;
        }

        @Override
        public void close() {
            // Nothing to release; each stream is closed by its reader.
        }
    }

    /** Zip-archive source; tolerates feeds nested one folder deep. */
    final class ZipGtfsSource implements GtfsSource {
        private final ZipFile zip;

        ZipGtfsSource(ZipFile zip) {
            this.zip = zip;
        }

        private ZipEntry find(String fileName) {
            ZipEntry direct = zip.getEntry(fileName);
            if (direct != null) {
                return direct;
            }
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry e = entries.nextElement();
                if (e.getName().endsWith("/" + fileName)) {
                    return e;
                }
            }
            return null;
        }

        @Override
        public boolean has(String fileName) {
            return find(fileName) != null;
        }

        @Override
        public InputStream open(String fileName) throws IOException {
            ZipEntry e = find(fileName);
            return e == null ? null : zip.getInputStream(e);
        }

        @Override
        public void close() throws IOException {
            zip.close();
        }
    }
}
