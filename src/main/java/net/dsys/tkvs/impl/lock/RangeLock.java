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

import java.util.Iterator;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.api.exception.Bug;
import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.lock.Lock;
import net.dsys.tkvs.api.lock.Locker;
import net.dsys.tkvs.api.transaction.TID;
import net.dsys.tkvs.impl.lock.IntervalTreeMap.Tuple;

/**
 * A single-threaded implementation of transaction locking. The semantics of
 * this lock are:
 * <ol>
 * <li>Many TIDs can share the write lock</li>
 * <li>Many TIDs can share the read lock</li>
 * <li>If there is only one reader, then that reader can upgrade its lock to
 * write</li>
 * <li>A writer can also read</li>
 * </ol>
 * 
 * @author Ricardo Padilha
 */
final class RangeLock {

	/**
	 * Default size for tree retrieval, i.e., when calling tree.getAll().
	 */
	private static final int DEFAULT_QUEUE_SIZE = 16;

	private static final Function<Tuple<Key, TX>, TX> TUPLE2TX = new Function<Tuple<Key, TX>, TX>() {
		@Override
		public TX apply(final Tuple<Key, TX> input) {
			return input.getValue();
		}
	};

	private final IntervalTreeMap<Key, TX> tree;
	private final int queueSize;

	RangeLock() {
		this.tree = new IntervalTreeMap<>();
		this.queueSize = DEFAULT_QUEUE_SIZE;
	}

	/**
	 * @see Locker#readRangeLock(Key, Key)
	 */
	void readLock(@Nonnull final Key start, @Nonnull final Key end, @Nonnull final TID tid,
			final long timestamp, @Nonnull final Lock counter) {
		final SortableDeque<Tuple<Key, TX>> queue = new SortableDeque<>(queueSize);
		tree.getAll(start, end, queue);
		queue.sort();
		final Tuple<Key, TX> tuple = queue.peekLast();
		// do we have a lock for this specific range and tid? if so, return.
		if (equals(tuple, start, end, tid)) {
			return;
		}
		// create a new lock entry
		final TX tx = TX.createReader(tid, timestamp, counter);
		// Since the queue is just a view of the tree,
		// then the counter reflects the count of conflicts.
		final int conflicts = countWriters(queue);
		tx.addTreeConflicts(conflicts);
		tree.put(start, end, tx);
	}

	/**
	 * @see Locker#writeRangeLock(Key, Key)
	 */
	void writeLock(@Nonnull final Key start, @Nonnull final Key end, @Nonnull final TID tid,
			final long timestamp, @Nonnull final Lock counter) {
		final SortableDeque<Tuple<Key, TX>> queue = new SortableDeque<>(queueSize);
		tree.getAll(start, end, queue);
		queue.sort();
		final Tuple<Key, TX> tuple = queue.peekLast();
		if (equals(tuple, start, end, tid)) {
			final TX tx = queue.pollLast().getValue();
			if (tx.isReader()) {
				// promote lock type
				tx.promoteType();
				final int conflicts = countReaders(queue);
				tx.addTreeConflicts(conflicts);
			}
			return;
		}
		// !equals(tuple, start, end, tid)
		final TX tx = TX.createWriter(tid, timestamp, counter);
		// Since the queue is just a view of the tree,
		// then the counter reflects the count of conflicts.
		final int conflicts = queue.size();
		tx.addTreeConflicts(conflicts);
		tree.put(start, end, tx);
	}

	void update(@Nonnull final Key start, @Nonnull final Key end, @Nonnull final TID tid,
			final long timestamp, @Nonnull final Set<TID> executables) {
		if (tree.isEmpty()) {
			// unlock when queue is empty?
			throw new Bug("update when tree is empty");
		}
		final SortableDeque<Tuple<Key, TX>> queue = new SortableDeque<>(queueSize);
		tree.getAll(start, end, queue);
		if (queue.isEmpty()) {
			// we have to update but this TID is not present?
			throw new Bug("update timestamp on missing tid: " + tid);
		}
		queue.sort();
		final Tuple<Key, TX> from = IntervalTreeMap.asTuple(start, end, new TX(tid));
		final Iterator<TX> it = queue.iterator(from, TUPLE2TX);
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
		if (readerHasDependentTx(tx, it)) {
			final int conflicts = updateReaderDependencies(tx.tid(), it, timestamp, executables);
			tx.addTreeConflicts(conflicts);
		}
		if (writerHasDependentTx(tx, it)) {
			final int conflicts = updateWriterDependencies(tx.tid(), it, timestamp, executables);
			tx.addTreeConflicts(conflicts);
		}
		// update the timestamp by removing and reinserting the transaction
		tree.remove(start, end, tx);
		tx.updateTimestamp(timestamp);
		tree.put(start, end, tx);
		queue.sort();
		// get the new first element
		final Tuple<Key, TX> newFirst = queue.peekFirst();
		// queue order not changed
		if (equals(newFirst, start, end, tid)) {
			// this is the result of an order request that did not change the
			// actual order
			if (tx.isExecutable()) {
				executables.add(tid);
			}
		}
	}

	void unlock(@Nonnull final Key start, @Nonnull final Key end, @Nonnull final TID tid,
			@Nonnull final Set<TID> executables) {
		if (tree.isEmpty()) {
			// unlock when queue is empty?
			throw new Bug("unlock when queue is empty");
		}
		final SortableDeque<Tuple<Key, TX>> queue = new SortableDeque<>(queueSize);
		tree.getAll(start, end, queue);
		if (queue.isEmpty()) {
			// we have to update but this TID is not present?
			throw new Bug("update timestamp on missing tid: " + tid);
		}
		queue.sort();
		final Tuple<Key, TX> from = IntervalTreeMap.asTuple(start, end, new TX(tid));
		final Iterator<TX> it = queue.iterator(from, TUPLE2TX);
		final TX tx = it.next();
		if (tx == null || !tx.tid().equals(tid)) {
			// iterator not starting on the proper TID
			throw new Bug("wrong tid iterator: " + tid);
		}
		// remove the transaction
		tree.remove(start, end, tx);
		it.remove();
		// update dependent transactions
		if (readerHasDependentTx(tx, it)) {
			unlockReaderDependencies(tx.tid(), it, executables);
		}
		if (writerHasDependentTx(tx, it)) {
			unlockWriterDependencies(tx.tid(), it, executables);
		}
	}

	void clear() {
		tree.clear();
	}

	@Nonnegative
	int size() {
		return tree.size();
	}

	boolean isEmpty() {
		return tree.isEmpty();
	}

	String toGraph() {
		final Iterator<TX> it = tree.iterator();
		if (!it.hasNext()) {
			return "";
		}
		final StringBuilder sb = new StringBuilder(1024);
		sb.append("subgraph ranges {\n");
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

	/**
	 * @return <code>true</code> if the tuple matches the start key, end key,
	 *         and tid
	 */
	private static boolean equals(@Nonnull final Tuple<Key, TX> tuple, @Nonnull final Key start,
			@Nonnull final Key end, @Nonnull final TID tid) {
		if (tuple == null) {
			return false;
		}
		return tuple.getStart().equals(start) && tuple.getEnd().equals(end) && tuple.getValue().tid().equals(tid);
	}

	/**
	 * Count the number of writers by removing all elements from the queue.
	 */
	private static int countWriters(@Nonnull final SortableDeque<Tuple<Key, TX>> queue) {
		int writers = 0;
		Tuple<Key, TX> tuple;
		while ((tuple = queue.pollFirst()) != null) {
			if (tuple.getValue().isWriter()) {
				writers++;
			}
		}
		return writers;
	}

	/**
	 * Count the number of readers by removing all elements from the queue.
	 */
	private static int countReaders(@Nonnull final SortableDeque<Tuple<Key, TX>> queue) {
		int readers = 0;
		Tuple<Key, TX> tuple;
		while ((tuple = queue.pollFirst()) != null) {
			if (tuple.getValue().isReader()) {
				readers++;
			}
		}
		return readers;
	}

	private static boolean readerHasDependentTx(@Nonnull final TX tx, @Nonnull final Iterator<TX> it) {
		return (tx.isReader()) && it.hasNext();
	}

	private static int updateReaderDependencies(@Nonnull final TID tid, @Nonnull final Iterator<TX> it,
			final long timestamp, @Nonnull final Set<TID> executables) {
		int conflicts = 0;
		// if we're updating a READER, then only decrease WRITERs
		while (it.hasNext()) {
			final TX next = it.next();
			if (next.tid().equals(tid)) {
				continue;
			}
			// limit ordering effects
			if (next.timestamp() > timestamp) {
				break;
			}
			if (next.isWriter()) {
				conflicts++;
				if (next.removeTreeConflict()) {
					executables.add(next.tid());
				}
			}
		}
		return conflicts;
	}

	private static void unlockReaderDependencies(@Nonnull final TID tid, @Nonnull final Iterator<TX> it,
			@Nonnull final Set<TID> executables) {
		// if we're unlocking a READER, then only decrease WRITERs
		while (it.hasNext()) {
			final TX next = it.next();
			if (next.tid().equals(tid)) {
				continue;
			}
			if (next.isWriter()) {
				if (next.removeTreeConflict()) {
					executables.add(next.tid());
				}
			}
		}
	}

	private static boolean writerHasDependentTx(@Nonnull final TX tx, @Nonnull final Iterator<TX> it) {
		return (tx.isWriter()) && it.hasNext();
	}

	private static int updateWriterDependencies(@Nonnull final TID tid, @Nonnull final Iterator<TX> it,
			final long timestamp, @Nonnull final Set<TID> executables) {
		int conflicts = 0;
		// if we're updating a WRITER, then decrease all next
		while (it.hasNext()) {
			final TX next = it.next();
			if (next.tid().equals(tid)) {
				continue;
			}
			// limit ordering effects
			if (next.timestamp() > timestamp) {
				break;
			}
			conflicts++;
			if (next.removeTreeConflict()) {
				executables.add(next.tid());
			}
		}
		return conflicts;
	}

	private static void unlockWriterDependencies(@Nonnull final TID tid, @Nonnull final Iterator<TX> it,
			@Nonnull final Set<TID> executables) {
		// if we're unlocking a WRITER, then decrease all next
		while (it.hasNext()) {
			final TX next = it.next();
			if (next.tid().equals(tid)) {
				continue;
			}
			if (next.removeTreeConflict()) {
				executables.add(next.tid());
			}
		}
	}
}
