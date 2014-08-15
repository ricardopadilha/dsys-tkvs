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

package net.dsys.tkvs.api.data;

import java.io.Serializable;

import javax.annotation.Nullable;

/**
 * @author Ricardo Padilha
 */
public final class Holder<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	@Nullable
	private T value;

	public Holder() {
	}

	public Holder(@Nullable final T value) {
		setValue(value);
	}

	public void setValue(@Nullable final T value) {
		this.value = value;
	}

	@Nullable
	public T getValue() {
		return value;
	}

	@Override
	public String toString() {
		return String.valueOf(value);
	}
}
