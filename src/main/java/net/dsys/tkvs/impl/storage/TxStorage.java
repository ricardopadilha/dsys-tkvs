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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.dsys.commons.api.exception.Bug;
import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.data.Value;
import net.dsys.tkvs.api.storage.InitializableStorage;
import net.dsys.tkvs.api.storage.Storage;
import net.dsys.tkvs.api.storage.TransactionalStorage;
import net.dsys.tkvs.api.transaction.TID;
import net.dsys.tkvs.impl.data.Values;

/**
 * This class implements a simple transactional storage.
 * <p>
 * This implementation has no locking, and assumes single-threaded access.
 * 
 * @author Ricardo Padilha
 */
final class TxStorage implements TransactionalStorage, InitializableStorage {

	@Nonnull
	private final Storage storage;
	@Nonnull
	private final HashMap<TID, TransactionBuffer> pending;

	@Nullable
	private TID currentTID;
	@Nullable
	private TransactionBuffer currentState;

	TxStorage(@Nonnull final Storage storage, @Nonnegative final int pendingCapacity) {
		this.storage = storage;
		this.pending = new HashMap<>(pendingCapacity, 1f);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void load(final Map<Key, Value> initialContent) {
		if (initialContent == null) {
			throw new NullPointerException("initialContent == null");
		}
		if (storage instanceof InitializableStorage) {
			((InitializableStorage) storage).load(initialContent);
			return;
		}
		throw new Bug("storage is not instanceof InitializableStorage");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void start(final TID tid) {
		if (tid == null) {
			throw new NullPointerException();
		}
		this.currentTID = tid;
		this.currentState = new TransactionBuffer();
		pending.put(currentTID, currentState);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean exists(final Key key) {
		return storage.exists(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Value read(final Key key) {
		if (currentState.isDeleted(key)) {
			return Values.nullValue();
		}
		if (currentState.isWritten(key)) {
			return currentState.read(key);
		}
		return storage.read(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key next(final Key key, final boolean includeKey) {
		return storage.next(key, includeKey);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key previous(final Key key, final boolean includeKey) {
		return storage.previous(key, includeKey);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Key key, final Value value) {
		currentState.write(key, value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete(final Key key) {
		currentState.delete(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		currentState.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void end() {
		currentTID = null;
		currentState = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void finish(final TID tid, final boolean status) {
		if (tid == null) {
			throw new NullPointerException("tid == null");
		}
		final TransactionBuffer state = pending.remove(tid);
		if (state == null) {
			throw new IllegalStateException("missing state for " + tid);
		}
		if (status) {
			commit(state);
		}
	}

	private void commit(@Nonnull final TransactionBuffer state) {
		if (state.cleared()) {
			storage.clear();
			return;
		}
		if (state.isEmpty()) {
			return;
		}
		for (final Entry<Key, Value> e : state.writes()) {
			storage.write(e.getKey(), e.getValue());
		}
		for (final Key k : state.deletes()) {
			storage.delete(k);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reset() {
		pending.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		reset();
		storage.close();
	}

	/**
	 * @author Ricardo Padilha
	 */
	private static final class TransactionBuffer {

		@Nonnull
		private final Map<Key, Value> writes;
		@Nonnull
		private final Set<Key> deletes;
		private boolean clear;

		TransactionBuffer() {
			this.writes = new HashMap<>();
			this.deletes = new HashSet<>();
		}

		void write(@Nonnull final Key key, @Nonnull final Value value) {
			writes.put(key, value);
		}

		boolean isWritten(@Nonnull final Key key) {
			return writes.containsKey(key);
		}

		@Nullable
		Value read(@Nonnull final Key key) {
			return writes.get(key);
		}

		@Nonnull
		Set<Entry<Key, Value>> writes() {
			return writes.entrySet();
		}

		void delete(@Nonnull final Key key) {
			deletes.add(key);
		}

		boolean isDeleted(@Nonnull final Key key) {
			return deletes.contains(key);
		}

		@Nonnull
		Set<Key> deletes() {
			return deletes;
		}

		void clear() {
			this.clear = true;
		}

		boolean cleared() {
			return clear;
		}

		boolean isEmpty() {
			return writes.isEmpty() && deletes.isEmpty();
		}
	}
}
