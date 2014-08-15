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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.api.exception.Bug;
import net.dsys.tkvs.api.lock.Lock;
import net.dsys.tkvs.api.transaction.TID;

/**
 * @author Ricardo Padilha
 */
final class TX implements Comparable<TX> {

	@Nonnull
	private final TID tid;
	@Nonnull
	private final Lock lock;
	private boolean queueConflict;
	private long timestamp;
	@Nonnull
	private TXType type;

	private TX(@Nonnull final TID tid, final long timestamp, @Nonnull final Lock lock,
			@Nonnull final TXType type) {
		if (tid == null) {
			throw new NullPointerException("tid == null");
		}
		if (lock == null) {
			throw new NullPointerException("lock == null");
		}
		if (type == null) {
			throw new NullPointerException("type == null");
		}
		this.tid = tid;
		this.lock = lock;
		this.timestamp = timestamp;
		this.queueConflict = false;
		this.type = type;
	}

	/**
	 * Used only to unlock.
	 */
	TX(@Nonnull final TID tid) {
		if (tid == null) {
			throw new NullPointerException("tid == null");
		}
		this.tid = tid;
		this.lock = new Counter();
		this.timestamp = Long.MIN_VALUE;
		this.queueConflict = false;
		this.type = TXType.WRITER;
	}

	@Nonnull
	TID tid() {
		return tid;
	}

	long timestamp() {
		return timestamp;
	}

	void updateTimestamp(final long timestamp) {
		this.timestamp = timestamp;
	}

	boolean isReader() {
		return type == TXType.READER;
	}

	boolean isWriter() {
		return type == TXType.WRITER;
	}

	void promoteType() {
		type = TXType.WRITER;
	}

	boolean isExecutable() {
		return !queueConflict && lock.isFree();
	}

	boolean hasQueueConflict() {
		return queueConflict;
	}

	void setQueueConflict() {
		if (!queueConflict) {
			queueConflict = true;
			lock.acquire();
		}
	}

	boolean unsetQueueConflict() {
		if (queueConflict) {
			queueConflict = false;
			if (lock.isFree()) {
				throw new Bug("mismatching local and global counters");
			}
			lock.release();
			return lock.isFree();
		}
		return false;
	}

	void addTreeConflicts(@Nonnegative final int n) {
		lock.acquire(n);
	}

	boolean removeTreeConflict() {
		if (lock.isFree()) {
			throw new Bug("mismatched tree conflict counting");
		}
		lock.release();
		return lock.isFree();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final TX o) {
		if (o == null) {
			return 1;
		}
		if (o == this) {
			return 0;
		}
		if (timestamp >= 0 && o.timestamp >= 0) {
			return Long.compare(timestamp, o.timestamp);
		}
		return tid.compareTo(o.tid);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj instanceof TX) {
			return tid.equals(((TX) obj).tid);
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return tid.hashCode();
	}

	static TX createReader(@Nonnull final TID tid, final long timestamp, @Nonnull final Lock counter) {
		return new TX(tid, timestamp, counter, TXType.READER);
	}

	static TX createWriter(@Nonnull final TID tid, final long timestamp, @Nonnull final Lock counter) {
		return new TX(tid, timestamp, counter, TXType.WRITER);
	}

	private static enum TXType {
		READER,
		WRITER;
	}
}
