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

package net.dsys.tkvs.impl.lock;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.Set;
import java.util.WeakHashMap;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.api.exception.Bug;
import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.lock.Lock;
import net.dsys.tkvs.api.lock.TransactionalLocker;
import net.dsys.tkvs.api.transaction.TID;
import net.dsys.tkvs.impl.data.Keys;

/**
 * @author Ricardo Padilha
 */
final class KeyRangeLocker implements TransactionalLocker {

	private static final Key FIRST_KEY = Keys.firstKey();
	private static final Key LAST_KEY = Keys.lastKey();

	private final Map<Key, KeyLock> keyLocks;
	private final RangeLock rangeLocks;
	private final Map<TID, LockingState> pending;

	private TID currentTID;
	private long currentTimestamp;
	private Lock currentLock;
	private LockingState currentState;

	KeyRangeLocker(@Nonnegative final int initialCapacity, @Nonnegative final int pendingCapacity) {
		this.keyLocks = new WeakHashMap<>(initialCapacity, 1f);
		this.rangeLocks = new RangeLock();
		this.pending = new HashMap<>(pendingCapacity, 1f);
	}

	private KeyLock getKeyLock(@Nonnull final Key key) {
		KeyLock lock = keyLocks.get(key);
		if (lock == null) {
			lock = new KeyLock();
			keyLocks.put(key, lock);
		}
		return lock;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void start(final TID tid, final long timestamp, final Lock lock) {
		if (tid == null || lock == null) {
			throw new NullPointerException();
		}
		this.currentTID = tid;
		this.currentTimestamp = timestamp;
		this.currentLock = lock;
		this.currentState = new LockingState();
		pending.put(currentTID, currentState);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readLock(final Key key) {
		if (Keys.isNull(key) || Keys.isAny(key) || Keys.isMeta(key)) {
			throw new IllegalArgumentException(String.valueOf(key));
		}
		final KeyLock lock = getKeyLock(key);
		lock.readLock(currentTID, currentTimestamp, currentLock);
		currentState.addKey(lock);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeLock(final Key key) {
		if (Keys.isNull(key) || Keys.isAny(key) || Keys.isMeta(key)) {
			throw new IllegalArgumentException(String.valueOf(key));
		}
		final KeyLock lock = getKeyLock(key);
		lock.writeLock(currentTID, currentTimestamp, currentLock);
		currentState.addKey(lock);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readRangeLock(final Key start, final Key end) {
		if (Keys.isNull(start, end) || Keys.isAny(start, end)) {
			throw new IllegalArgumentException(String.valueOf(start) + ":" + String.valueOf(end));
		}
		rangeLocks.readLock(start, end, currentTID, currentTimestamp, currentLock);
		currentState.addRange(start, end);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRangeLock(final Key start, final Key end) {
		if (Keys.isNull(start, end) || Keys.isAny(start, end)) {
			throw new IllegalArgumentException(String.valueOf(start) + ":" + String.valueOf(end));
		}
		rangeLocks.writeLock(start, end, currentTID, currentTimestamp, currentLock);
		currentState.addRange(start, end);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeAllLock() {
		rangeLocks.writeLock(FIRST_KEY, LAST_KEY, currentTID, currentTimestamp, currentLock);
		currentState.addRange(FIRST_KEY, LAST_KEY);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void end() {
		currentTID = null;
		currentTimestamp = 0;
		currentLock = null;
		currentState = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void update(final TID tid, final long timestamp, final Set<TID> executables) {
		final LockingState state = pending.get(tid);
		if (state == null) {
			throw new Bug("state not found for update: " + tid);
		}
		for (final KeyRange range : state.ranges()) {
			rangeLocks.update(range.start(), range.end(), tid, timestamp, executables);
		}
		for (final KeyLock lock : state.keys()) {
			lock.update(tid, timestamp, executables);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unlock(final TID tid, final Set<TID> executables, final boolean commit) {
		if (tid == null) {
			throw new NullPointerException("tid == null");
		}
		if (executables == null) {
			throw new NullPointerException("executables == null");
		}
		final LockingState state = pending.remove(tid);
		if (state == null) {
			throw new Bug("Missing LockingState for " + tid);
		}
		for (final KeyRange kr : state.ranges()) {
			rangeLocks.unlock(kr.start(), kr.end(), tid, executables);
		}
		for (final KeyLock lock : state.keys()) {
			lock.unlock(tid, executables, commit);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {
		return pending.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reset() {
		keyLocks.clear();
		rangeLocks.clear();
		pending.clear();
	}

	String toGraph() {
		final StringBuilder sb = new StringBuilder(1024);
		sb.append("digraph map");
		sb.append(this.hashCode());
		sb.append(" {\n");
		final String subgraph1 = rangeLocks.toGraph();
		if (subgraph1.length() > 0) {
			sb.append(subgraph1).append('\n');
		}
		for (final Entry<Key, KeyLock> e : keyLocks.entrySet()) {
			final String name = e.getKey().toString();
			final String subgraph = e.getValue().toGraph(name);
			if (subgraph.length() > 0) {
				sb.append(subgraph).append('\n');
			}
		}
		sb.append("}");
		return sb.toString();
	}

	/**
	 * @author Ricardo Padilha
	 */
	private static final class LockingState {

		private final Set<KeyRange> ranges;
		private final Set<KeyLock> keys;

		LockingState() {
			this.ranges = new HashSet<>();
			this.keys = new HashSet<>();
		}

		void addRange(@Nonnull final Key start, @Nonnull final Key end) {
			ranges.add(new KeyRange(start, end));
		}

		void addKey(@Nonnull final KeyLock key) {
			keys.add(key);
		}

		Set<KeyRange> ranges() {
			return ranges;
		}

		Set<KeyLock> keys() {
			return keys;
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	private static final class KeyRange {

		private final Key start;
		private final Key end;
		private final int hashCode;

		KeyRange(@Nonnull final Key start, @Nonnull final Key end) {
			if (start == null || end == null) {
				throw new NullPointerException();
			}
			this.start = start;
			this.end = end;
			this.hashCode = Objects.hash(start, end);
		}

		Key start() {
			return start;
		}

		Key end() {
			return end;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int hashCode() {
			return hashCode;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean equals(final Object obj) {
			if (obj == this) {
				return true;
			}
			if (obj instanceof KeyRange) {
				final KeyRange ki = (KeyRange) obj;
				return start.equals(ki.start)
					&& end.equals(ki.end);
			}
			return false;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String toString() {
			return "[" + start + ", " + end + "]";
		}
	}
}
