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

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import javax.annotation.Nonnull;

import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.data.Value;
import net.dsys.tkvs.api.storage.InitializableStorage;
import net.dsys.tkvs.impl.data.Keys;

/**
 * This class implements a simple in memory storage.
 * <p>
 * This implementation has no locking, and assumes single-threaded access.
 * 
 * @author Ricardo Padilha
 */
final class MapStorage implements InitializableStorage {

	@Nonnull
	private final NavigableMap<Key, Key> keys;
	@Nonnull
	private final Map<Key, Value> values;
	private final boolean fakeWrites;

	MapStorage(@Nonnull final NavigableMap<Key, Key> keys, @Nonnull final Map<Key, Value> values) {
		this(keys, values, false);
	}

	MapStorage(@Nonnull final NavigableMap<Key, Key> keys, @Nonnull final Map<Key, Value> values,
			final boolean fakeWrites) {
		if (values == null) {
			throw new NullPointerException("map == null");
		}
		if (keys == null) {
			throw new NullPointerException("keys == null");
		}
		this.keys = keys;
		this.values = values;
		this.fakeWrites = fakeWrites;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void load(final Map<Key, Value> initialContent) {
		if (initialContent == null) {
			throw new NullPointerException("initialContent == null");
		}
		for (final Entry<Key, Value> e : initialContent.entrySet()) {
			final Key key = e.getKey();
			values.put(key, e.getValue());
			keys.put(key, key);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean exists(final Key key) {
		if (Keys.isAny(key)) {
			return !values.isEmpty();
		}
		return values.containsKey(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Value read(final Key key) {
		return values.get(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key next(final Key key, final boolean includeKey) {
		if (includeKey) {
			return keys.ceilingKey(key);
		}
		return keys.higherKey(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key previous(final Key key, final boolean includeKey) {
		if (includeKey) {
			return keys.floorKey(key);
		}
		return keys.lowerKey(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Key key, final Value value) {
		if (!fakeWrites) {
			if (!values.containsKey(key)) {
				keys.put(key, key);
			}
			values.put(key, value);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete(final Key key) {
		keys.remove(key);
		values.remove(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		keys.clear();
		values.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		clear();
	}
}
