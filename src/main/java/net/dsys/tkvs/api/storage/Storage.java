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

/**
 * @author Ricardo Padilha
 */
public interface Storage {

	/**
	 * Checks the existence of a value.
	 */
	boolean exists(@Nonnull Key key);

	/**
	 * Read a single value.
	 */
	@Nonnull
	Value read(@Nonnull Key key);

	/**
	 * Get the next key.
	 */
	@Nonnull
	Key next(@Nonnull Key key, boolean includeKey);

	/**
	 * Get the previous key. Accepts concrete or meta keys.
	 */
	@Nonnull
	Key previous(@Nonnull Key key, boolean includeKey);

	/**
	 * Writes/inserts a single value. Concrete keys only.
	 */
	void write(@Nonnull Key key, @Nonnull Value value);

	/**
	 * Deletes a single key. Accepts concrete or meta keys.
	 */
	void delete(@Nonnull Key key);

	/**
	 * Clears the content of the storage.
	 */
	void clear();

	/**
	 * Closes the storage, releasing all resources.
	 */
	void close();

}
