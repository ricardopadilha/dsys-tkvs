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

package net.dsys.tkvs.api.storage;

import javax.annotation.Nonnull;

import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.data.Value;
import net.dsys.tkvs.api.transaction.TID;

/**
 * @author Ricardo Padilha
 */
public interface TransactionalStorage extends Storage {

	/**
	 * Starts the processing of a single transaction.
	 */
	void start(@Nonnull TID tid);

	/**
	 * Read a single value using the transaction buffer.
	 * If the value is not on the transaction buffer,
	 * then read it from storage.
	 */
	@Override
	Value read(@Nonnull Key key);

	/**
	 * Finishes the processing of the transaction started by {@link #start(TID)}.
	 */
	void end();

	/**
	 * Commits or aborts the given transaction.
	 */
	void finish(@Nonnull TID tid, boolean commit);

	/**
	 * Clear all pending transactions.
	 */
	void reset();

}
