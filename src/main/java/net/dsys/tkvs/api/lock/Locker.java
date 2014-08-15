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

package net.dsys.tkvs.api.lock;

import javax.annotation.Nonnull;

import net.dsys.tkvs.api.data.Key;

/**
 * @author Ricardo Padilha
 */
public interface Locker {

	/**
	 * Locks a single concrete key for reading.
	 */
	void readLock(@Nonnull Key key);

	/**
	 * Locks a single concrete key.
	 */
	void writeLock(@Nonnull Key key);

	/**
	 * Locks a range for reading.
	 */
	void readRangeLock(@Nonnull Key start, @Nonnull Key end);

	/**
	 * Locks a range for writing.
	 */
	void writeRangeLock(@Nonnull Key start, @Nonnull Key end);

	/**
	 * Locks all database.
	 */
	void writeAllLock();

	/**
	 * Size of current pending transactions.
	 */
	int size();

	/**
	 * Clear all pending locks.
	 */
	void reset();

}
