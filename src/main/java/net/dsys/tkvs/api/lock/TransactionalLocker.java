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

import java.util.Set;

import javax.annotation.Nonnull;

import net.dsys.tkvs.api.transaction.TID;

/**
 * @author Ricardo Padilha
 */
public interface TransactionalLocker extends Locker {

	/**
	 * Starts the processing of a single transaction.
	 */
	void start(@Nonnull TID tid, long timestamp, @Nonnull Lock lock);

	/**
	 * Finishes the processing of the transaction started by {@link #start(TID)}
	 * and returns the set of conflicts.
	 */
	void end();

	/**
	 * Updates the timestamp of the given transaction.
	 */
	void update(@Nonnull TID tid, long timestamp, @Nonnull Set<TID> executables);

	/**
	 * Releases the locks for the given transaction.
	 */
	void unlock(@Nonnull TID tid, @Nonnull Set<TID> executables, boolean commit);

}
