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

import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.dsys.commons.api.exception.Bug;
import net.dsys.tkvs.api.lock.Lock;
import net.dsys.tkvs.api.transaction.TID;

/**
 * A single-threaded implementation of transaction locking. The semantics of
 * this lock are:
 * <ol>
 * <li>Only one TID can hold the write lock</li>
 * <li>Many TIDs can share the read lock</li>
 * <li>If there is only one reader, then that reader can upgrade its lock to
 * write</li>
 * <li>A writer can also read</li>
 * </ol>
 * 
 * @author Ricardo Padilha
 */
final class KeyLock {

	/**
	 * Default size for lock queue.
	 */
	private static final int DEFAULT_QUEUE_SIZE = 16;

	private static final Function<TX, TID> TX2TID = new Function<TX, TID>() {
		@Override
		public TID apply(final TX input) {
			return input.tid();
		}
	};

	private final SortableDeque<TX> queue;

	KeyLock() {
		this(DEFAULT_QUEUE_SIZE);
	}

	KeyLock(@Nonnegative final int size) {
		this.queue = new SortableDeque<>(size);
	}

	void readLock(@Nonnull final TID tid, final long timestamp, @Nonnull final Lock counter) {
		final TX last = queue.peekLast();
		if (equals(last, tid)) {
			return;
		}
		final TX tx = TX.createReader(tid, timestamp, counter);
		if (isReadConflict(last)) {
			tx.setQueueConflict();
		}
		queue.pushLast(tx);
	}

	void writeLock(@Nonnull final TID tid, final long timestamp, @Nonnull final Lock counter) {
		final TX last = queue.peekLast();
		if (equals(last, tid)) {
			if (last.isReader()) {
				// promote lock type
				last.promoteType();
				if (queue.size() > 1) {
					last.setQueueConflict();
				}
			}
			return;
		}
		final TX tx = TX.createWriter(tid, timestamp, counter);
		if (!queue.isEmpty()) {
			tx.setQueueConflict();
		}
		queue.pushLast(tx);
	}

	void update(@Nonnull final TID tid, final long timestamp, @Nonnull final Set<TID> executables) {
		if (queue.isEmpty()) {
			// unlock when queue is empty?
			throw new Bug("update when queue is empty");
		}
		final IteratorCurrent<TX> it = queue.iterator(new TX(tid));
		if (!it.hasNext()) {
			// we have to update but this TID is not present?
			throw new Bug("update timestamp on missing tid: " + tid);
		}
		final TX curr = it.current();
		final TX tx = it.next();
		if (!tx.tid().equals(tid)) {
			// iterator not starting on the proper TID
			throw new Bug("wrong tid iterator: " + tx.tid() + " != " + tid);
		}
		if (tx.timestamp() > timestamp) {
			// moving transaction to the past?
			throw new Bug("timestamp lower than current value: " + tx.timestamp() + " > " + timestamp);
		}
		// update dependent transactions
		if (readerHasDependentTx(curr, tx, it)) {
			updateReaderDependencies(it, timestamp, executables);
		}
		if (writerHasDependentTx(curr, tx, it)) {
			updateWriterDependencies(curr, it, timestamp, executables);
		}
		// update the timestamp and sort the queue
		tx.updateTimestamp(timestamp);
		queue.sort();
		// get the new first element
		if (tx.equals(queue.peekFirst())) {
			// queue order not changed
			if (tx.isExecutable()) {
				executables.add(tid);
			}
			return;
		}
		// queue order changed, therefore update tx conflict information
		if (tx.isWriter()) {
			// if a WRITER was moved, increment conflict counters
			tx.setQueueConflict();
		} else { // tx.type = READER
			// if a READER was moved, set its conflict counter
			// to the same as its preceding transaction
			final TX prev = queue.getPrevious(TX2TID, tid);
			if (prev == null) {
				// tx was not first, but there is no previous?
				throw new Bug("no tx before tid: " + tid);
			}
			if (!prev.hasQueueConflict() && tx.hasQueueConflict()) {
				throw new Bug("incompatible queue conflicts between tx and prev");
			}
			if (prev.hasQueueConflict()) {
				tx.setQueueConflict();
			}
		}
	}

	void unlock(@Nonnull final TID tid, @Nonnull final Set<TID> executables, final boolean commit) {
		if (queue.isEmpty()) {
			// unlock when queue is empty?
			throw new Bug("unlock when queue is empty");
		}
		final IteratorCurrent<TX> it = queue.iterator(new TX(tid));
		if (!it.hasNext()) {
			// unlock when TID is missing?
			throw new Bug("unlock missing tid: " + tid);
		}
		final TX curr = it.current();
		final TX tx = it.next();
		if (!tx.tid().equals(tid)) {
			// iterator not starting on the proper TID
			throw new Bug("wrong tid iterator: " + tx.tid() + " != " + tid);
		}
		if (commit && (tx.isWriter()) && (curr != null)) {
			// unlocking a writer in the middle of the queue
			throw new Bug("unlocking writer in the middle of the queue: " + tid);
		}
		// remove the transaction
		it.remove();
		// update dependent transactions
		if (readerHasDependentTx(curr, tx, it)) {
			unlockReaderDependencies(it, executables);
		}
		if (writerHasDependentTx(curr, tx, it)) {
			unlockWriterDependencies(curr, it, executables);
		}
	}

	void clear() {
		queue.clear();
	}

	@Nonnegative
	int size() {
		return queue.size();
	}

	boolean isEmpty() {
		return queue.isEmpty();
	}

	private static boolean equals(@Nonnull final TX tx, @Nonnull final TID tid) {
		if (tx == null) {
			return false;
		}
		return tx.tid().equals(tid);
	}

	private static boolean isReadConflict(@Nonnull final TX tx) {
		if (tx == null) {
			return false;
		}
		return tx.isWriter() || tx.hasQueueConflict();
	}

	private static boolean readerHasDependentTx(@Nullable final TX curr, @Nonnull final TX tx,
			@Nonnull final IteratorCurrent<TX> it) {
		return (tx.isReader()) && (curr == null) && it.hasNext();
	}

	private static void updateReaderDependencies(@Nonnull final IteratorCurrent<TX> it, final long timestamp,
			@Nonnull final Set<TID> executables) {
		// if we're updating a READER, then only unset the next
		final TX next = it.next();
		// limit ordering effects
		if (next.timestamp() < timestamp && next.unsetQueueConflict()) {
			executables.add(next.tid());
		}
	}

	private static void unlockReaderDependencies(@Nonnull final IteratorCurrent<TX> it,
			@Nonnull final Set<TID> executables) {
		// if we're unlocking a READER, then only unset the next
		final TX next = it.next();
		if (next.unsetQueueConflict()) {
			executables.add(next.tid());
		}
	}

	private static boolean writerHasDependentTx(@Nullable final TX curr, @Nonnull final TX tx,
			@Nonnull final IteratorCurrent<TX> it) {
		return (tx.isWriter()) && (curr == null || !curr.hasQueueConflict()) && it.hasNext();
	}

	private static void updateWriterDependencies(@Nullable final TX curr, @Nonnull final IteratorCurrent<TX> it,
			final long timestamp, @Nonnull final Set<TID> executables) {
		// if we're updating a WRITER, then unset all next READERS
		TX prev = curr;
		while (it.hasNext()) {
			final TX next = it.next();
			// limit ordering effects
			if (next.timestamp() > timestamp) {
				break;
			}
			if (next.isWriter()) {
				if ((prev == null) && next.unsetQueueConflict()) {
					executables.add(next.tid());
				}
				break;
			}
			// next.type == READER
			if (prev == null || (prev.isReader() && !prev.hasQueueConflict())) {
				if (next.unsetQueueConflict()) {
					executables.add(next.tid());
				}
			}
			prev = next;
		}

	}

	private static void unlockWriterDependencies(@Nullable final TX curr, @Nonnull final IteratorCurrent<TX> it,
			@Nonnull final Set<TID> executables) {
		// if we're unlocking a WRITER, then unset all next READERS
		TX prev = curr;
		while (it.hasNext()) {
			final TX next = it.next();
			if (next.isWriter()) {
				if ((prev == null) && next.unsetQueueConflict()) {
					executables.add(next.tid());
				}
				break;
			}
			// next.type == READER
			if (next.unsetQueueConflict()) {
				executables.add(next.tid());
			}
			prev = next;
		}
	}

	String toGraph(@Nonnull final String name) {
		final IteratorCurrent<TX> it = queue.iterator();
		if (!it.hasNext()) {
			return "";
		}
		final StringBuilder sb = new StringBuilder(1024);
		sb.append("subgraph key").append(name).append(" {\n");
		TX prev = null;
		for (; it.hasNext();) {
			final TX tx = it.next();
			if (prev != null) {
				sb.append("tid").append(prev.tid()).append(" -> ").append("tid").append(tx.tid()).append('\n');
			}
			prev = tx;
		}
		sb.append("}");
		return sb.toString();
	}
}
