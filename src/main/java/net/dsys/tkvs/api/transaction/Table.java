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

package net.dsys.tkvs.api.transaction;

import java.util.Map;

import javax.annotation.Nonnull;

import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.data.Value;

/**
 * @author Ricardo Padilha
 */
public interface Table {

	@Nonnull
	Boolean contains(@Nonnull Key key);

	@Nonnull
	Value read(@Nonnull Key key);

	@Nonnull
	Map<Key, Value> readRange(@Nonnull Key start, @Nonnull Boolean includeStart,
			@Nonnull Key end, @Nonnull Boolean includeEnd);

	@Nonnull
	Map<Key, Value> readRange(@Nonnull Key start, @Nonnull Boolean includeStart,
			@Nonnull Integer count, @Nonnull Direction direction);

	@Nonnull
	Key previous(@Nonnull Key key);

	@Nonnull
	Key next(@Nonnull Key key);

	void write(@Nonnull Key key, @Nonnull Value value);

	void delete(@Nonnull Key key);

}
