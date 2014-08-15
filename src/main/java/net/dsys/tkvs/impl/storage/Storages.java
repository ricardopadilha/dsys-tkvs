/**
 * Copyright 2014 Ricardo Padilha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dsys.tkvs.impl.storage;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.data.Value;
import net.dsys.tkvs.api.storage.Storage;
import net.dsys.tkvs.api.storage.TransactionalStorage;

/**
 * @author Ricardo Padilha
 */
public final class Storages {

	private Storages() {
		// no instantiation
	}

	@Nonnull
	public static TransactionalStorage getUnbound(@Nonnegative final int initialCapacity,
			@Nonnegative final int pendingCapacity) {
		final NavigableMap<Key, Key> keys = new TreeMap<>();
		final Map<Key, Value> values = new HashMap<>(initialCapacity);
		final Storage storage = new MapStorage(keys, values);
		final TransactionalStorage txstorage = new TxStorage(storage, pendingCapacity);
		return txstorage;
	}

	@Nonnull
	public static TransactionalStorage getBounded(@Nonnegative final int initialCapacity,
			@Nonnegative final int maxCapacity, @Nonnegative final int pendingCapacity) {
		final NavigableMap<Key, Key> keys = new TreeMap<>();
		final Map<Key, Value> values = new LimitedMap<>(initialCapacity, 1f, false, maxCapacity, keys);
		final Storage storage = new MapStorage(keys, values);
		final TransactionalStorage txstorage = new TxStorage(storage, pendingCapacity);
		return txstorage;
	}

	@Nonnull
	public static TransactionalStorage getMemoryDB(@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		return MapDBTxStorage.inMemory(dbName, pendingCapacity);
	}

	@Nonnull
	public static TransactionalStorage getTempFileDB(@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		return MapDBTxStorage.inTempFile(dbName, pendingCapacity);
	}

	@Nonnull
	public static TransactionalStorage getFileDB(@Nonnull final Path path,
			@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		return MapDBTxStorage.inFile(path, dbName, pendingCapacity);
	}

	@Nonnull
	public static TransactionalStorage getTempBerkleyDB(@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		return BerkleyDBTxStorage.inTempFile(dbName, pendingCapacity);
	}

	@Nonnull
	public static TransactionalStorage getBerkleyDB(@Nonnull final Path path,
			@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		return BerkleyDBTxStorage.inFile(path, dbName, pendingCapacity);
	}

	/**
	 * @author Ricardo Padilha
	 */
	private static final class LimitedMap<K, V> extends LinkedHashMap<K, V> {

		private static final long serialVersionUID = 1L;
		@Nonnegative
		private final int maxSize;
		@Nonnull
		private final Map<K, K> keys;

		LimitedMap(@Nonnegative final int initialCapacity,
				@Nonnegative final float loadFactor,
				final boolean accessOrder,
				@Nonnegative final int maxSize,
				@Nonnull final Map<K, K> keys) {
			super(initialCapacity, loadFactor, accessOrder);
			if (maxSize < initialCapacity) {
				throw new IllegalArgumentException("maxSize < initialCapacity");
			}
			this.maxSize = maxSize;
			this.keys = keys;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected boolean removeEldestEntry(final Entry<K, V> eldest) {
			if (size() >= maxSize) {
				keys.remove(eldest.getKey());
				return true;
			}
			return false;
		}
	}
}
