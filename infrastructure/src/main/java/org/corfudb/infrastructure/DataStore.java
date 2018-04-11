package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import lombok.Data;
import lombok.Getter;

import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.util.JsonUtils;

import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;

/**
 * Stores data as JSON.
 *
 * <p>Handle in-memory and persistent case differently:
 *
 * <p>In in-memory mode, the "cache" is actually the store, so we never evict anything from it.
 *
 * <p>In persistent mode, we use a {@link LoadingCache}, where an in-memory map is backed by disk.
 * In this scheme, the key for each value is also the name of the file where the value is stored.
 * The key is determined as (prefix + "_" + key).
 * The cache here serves mostly for easily managed synchronization of in-memory/file.
 *
 * <p>If 'opts' either has '--memory=true' or a log-path for storing files is not provided,
 * the store is just an in memory cache.
 *
 * <p>Created by mdhawan on 7/27/16.
 */
public class DataStore implements IDataStore {

    static String EXTENSION = ".ds";

    private final Map<String, Object> opts;
    private final boolean isPersistent;
    @Getter
    private final LoadingCache<DataStoreKey, Object> cache;
    private final String logDir;

    @Getter
    private final long dsCacheSize = 1_000; // size bound for in-memory cache for dataStore

    /**
     * Return a new DataStore object.
     * @param opts  map of option strings
     */
    public DataStore(Map<String, Object> opts) {
        this.opts = opts;

        if ((opts.get("--memory") != null && (Boolean) opts.get("--memory"))
                || opts.get("--log-path") == null) {
            // in-memory dataSture case
            isPersistent = false;
            this.logDir = null;
            cache = buildMemoryDs();
        } else {
            // persistent dataSture case
            isPersistent = true;
            this.logDir = (String) opts.get("--log-path");
            cache = buildPersistentDs();
        }
    }

    /**
     * obtain an in-memory cache, no content loader, no writer, no size limit.
     * @return  new LoadingCache for the DataStore
     */
    private LoadingCache<DataStoreKey, Object> buildMemoryDs() {
        LoadingCache<DataStoreKey, Object> cache = Caffeine
                .newBuilder()
                .build(k -> null);
        return cache;
    }


    public static int getChecksum(byte[] bytes) {
        Hasher hasher = Hashing.crc32c().newHasher();
        for (byte a : bytes) {
            hasher.putByte(a);
        }

        return hasher.hash().asInt();
    }

    /**
     * obtain a {@link LoadingCache}.
     * The cache is backed up by file-per-key uner {@link DataStore::logDir}.
     * The cache size is bounded by {@link DataStore::dsCacheSize}.
     *
     * @return the cache object
     */
    private LoadingCache<DataStoreKey, Object> buildPersistentDs() {

        LoadingCache<DataStoreKey, Object> cache = Caffeine.newBuilder()
                .recordStats()
                .writer(new CacheWriter<DataStoreKey, Object>() {
                    @Override
                    public synchronized void write(@Nonnull DataStoreKey dsKey, @Nonnull Object value) {
                        try {

                            Path path = Paths.get(logDir + File.separator + dsKey.getKey() + EXTENSION);
                            Path tmpPath = Paths.get(logDir + File.separator + dsKey.getKey() + EXTENSION + ".tmp");

                            String json = JsonUtils.parser.toJson(value, dsKey.getTClass());
                            byte[] stringBytes = json.getBytes();

                            ByteBuffer buffer = ByteBuffer.allocate(json.getBytes().length
                                    + Integer.BYTES);
                            buffer.putInt(getChecksum(stringBytes));
                            buffer.put(stringBytes);
                            Files.write(tmpPath, buffer.array(), StandardOpenOption.CREATE,
                                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
                            Files.move(tmpPath, path, StandardCopyOption.REPLACE_EXISTING,
                                    StandardCopyOption.ATOMIC_MOVE);
                            syncDirectory(logDir);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public synchronized void delete(@Nonnull DataStoreKey dsKey,
                                                    @Nullable Object value,
                                                    @Nonnull RemovalCause cause) {
                        try {
                            Path path = Paths.get(logDir + File.separator + dsKey.getKey());
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .maximumSize(dsCacheSize)
                .build(dsKey -> {
                    try {
                        Path path = Paths.get(logDir + File.separator + dsKey.getKey() + EXTENSION);
                        if (Files.notExists(path)) {
                            return null;
                        }
                        byte[] bytes = Files.readAllBytes(path);
                        ByteBuffer buf = ByteBuffer.wrap(bytes);
                        int checksum = buf.getInt();
                        byte[] jsonBytes = Arrays.copyOfRange(bytes, 4, bytes.length);
                        if (checksum != getChecksum(jsonBytes)) {
                            throw new DataCorruptionException();
                        }
                        return getObject(new String(jsonBytes), dsKey.getTClass());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        return cache;
    }

    @Override
    public synchronized <T> void put(Class<T> tclass, String prefix, String key, T value) {
        String json = JsonUtils.parser.toJson(value, tclass);
        T tmpVal = JsonUtils.parser.fromJson(json, tclass);
        DataStoreKey dsk = new DataStoreKey(getKey(prefix, key), tclass);
        cache.put(dsk, tmpVal);
    }

    @Override
    public synchronized  <T> T get(Class<T> tclass, String prefix, String key) {
        DataStoreKey dsk = new DataStoreKey(getKey(prefix, key), tclass);
        return (T) cache.get(dsk);
    }

    /**
     * This is an atomic conditional get/put: If the key is not found,
     * then the specified value is inserted.
     * It returns the latest value, either the original one found,
     * or the newly inserted value
     * @param tclass type of value
     * @param prefix prefix part of key
     * @param key suffice part of key
     * @param value value to be conditionally accepted
     * @param <T> value class
     * @return the latest value in the cache
     */
    public <T> T get(Class<T> tclass, String prefix, String key, T value) {
        DataStoreKey dsk = new DataStoreKey(getKey(prefix, key), tclass);
        return (T) cache.get(dsk, k -> {
            String json = JsonUtils.parser.toJson(value, tclass);
            T tmpVal = JsonUtils.parser.fromJson(json, tclass);
            return tmpVal;
        });
    }

    @Override
    public synchronized  <T> List<T> getAll(Class<T> tclass, String prefix) {
        List<T> list = new ArrayList<T>();
        for (Map.Entry<DataStoreKey, Object> entry : cache.asMap().entrySet()) {
            if (entry.getKey().getKey().startsWith(prefix)) {
                list.add((T) entry.getValue());
            }
        }
        return list;
    }

    @Override
    public synchronized <T> void delete(Class<T> tclass, String prefix, String key) {
        cache.invalidate(getKey(prefix, key));
    }

    // Helper methods

    private <T> T getObject(String json, Class<T> tclass) {
        return isNotNull(json) ? JsonUtils.parser.fromJson(json, tclass) : null;
    }

    private String getKey(String prefix, String key) {
        return prefix + "_" + key;
    }

    private boolean isNotNull(String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Data
    class DataStoreKey<T> {
        final String key;
        final Class<T> tClass;
    }

}
