/**
 * Copyright 2009 the original author or authors.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.javacrumbs.shedlock.provider.momento;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import momento.sdk.CacheClient;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.SetIfNotExistsResponse;
import net.javacrumbs.shedlock.core.AbstractSimpleLock;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import net.javacrumbs.shedlock.support.LockException;
import net.javacrumbs.shedlock.support.Utils;
import net.javacrumbs.shedlock.support.annotation.NonNull;

/**
 * Distributed lock using Momento. Depends on
 * <code>software.momento.java:sdk</code>.
 *
 * It stores a simple key-value for the lock where the key is the lock name and the value is the
 * hostname.
 */
public class MomentoLockProvider implements LockProvider {

    private static final int REQUEST_TIMEOUT_SECONDS = 10;

    private final String hostname;
    private final CacheClient cacheClient;
    private final String cacheName;

    /**
     * Uses Momento to coordinate locks
     *
     * @param cacheClient
     *            the lock cache client
     * @param cacheName
     *            the lock cache name
     */
    public MomentoLockProvider(@NonNull CacheClient cacheClient, @NonNull String cacheName) {
        this.cacheClient = requireNonNull(cacheClient, "cacheClient can not be null");
        this.cacheName = requireNonNull(cacheName, "cacheName can not be null");
        this.hostname = Utils.getHostname();
    }

    @Override
    @NonNull
    public Optional<SimpleLock> lock(@NonNull LockConfiguration lockConfiguration) {

        final Instant lockAtMostUntil = lockConfiguration.getLockAtMostUntil();
        final Duration ttl = Duration.between(Instant.now(), lockAtMostUntil);

        final SetIfNotExistsResponse setResponse;
        try {
             setResponse = cacheClient.setIfNotExists(cacheName, lockConfiguration.getName(), hostname, ttl)
                 .get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new LockException("Got unexpected exception while attempting to acquire lock " + lockConfiguration.getName());
        }

        if (setResponse instanceof SetIfNotExistsResponse.Stored) {
            return Optional.of(new MomentoLock(cacheClient, cacheName, lockConfiguration));
        } else if (setResponse instanceof SetIfNotExistsResponse.NotStored) {
            return Optional.empty();
        } else if (setResponse instanceof SetIfNotExistsResponse.Error){
            throw new LockException("Got error while attempting to acquire lock " + lockConfiguration.getName(),
                                        ((SetIfNotExistsResponse.Error) setResponse).getCause());
        }

        return Optional.empty();
    }

    private static final class MomentoLock extends AbstractSimpleLock {
        private final CacheClient cacheClient;
        private final String cacheName;

        private MomentoLock(CacheClient cacheClient, String cacheName, LockConfiguration lockConfiguration) {
            super(lockConfiguration);
            this.cacheClient = cacheClient;
            this.cacheName = cacheName;
        }

        @Override
        public void doUnlock() {

            final DeleteResponse deleteResponse;
            try {
                deleteResponse = this.cacheClient.delete(cacheName, lockConfiguration.getName())
                    .get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new LockException("Got unexpected exception while attempting to acquire lock " + lockConfiguration.getName());
            }

            if (deleteResponse instanceof DeleteResponse.Error) {
                throw new LockException("Got error while attempting to acquire lock " + lockConfiguration.getName(),
                                            ((DeleteResponse.Error) deleteResponse).getCause());
            }
        }
    }
}
