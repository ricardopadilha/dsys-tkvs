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

package net.dsys.tkvs.api.tkvs;

import java.io.Serializable;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * @author Ricardo Padilha
 */
public interface TableSerializable {

	@Nonnull
	<K extends Serializable> Boolean contains(@Nonnull K key);

	@Nonnull
	<K extends Serializable, V extends Serializable> V read(@Nonnull K key);

	@Nonnull
	<V extends Serializable> V read(@Nonnull Serializable... key);

	@Nonnull
	<K extends Serializable, V extends Serializable> Map<K, V> readRange(
			@Nonnull K start, @Nonnull Boolean includeStart, @Nonnull K end, @Nonnull Boolean includeEnd);

	@Nonnull
	<K extends Serializable, V extends Serializable> Map<K, V> readRange(
			@Nonnull K start, @Nonnull Boolean includeStart, @Nonnull Integer count, @Nonnull Direction direction);

	@Nonnull
	<K extends Serializable> K previous(@Nonnull K key);

	@Nonnull
	<K extends Serializable> K next(@Nonnull K key);

	<K extends Serializable, V extends Serializable> void write(@Nonnull K key, @Nonnull V value);

	<K extends Serializable> void delete(@Nonnull K key);

	void delete(@Nonnull Serializable... key);

}
