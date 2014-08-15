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

package net.dsys.tkvs.impl.data;

import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.data.Value;

/**
 * @author Ricardo Padilha
 */
final class NullValue implements Value {

	private static final long serialVersionUID = 1L;

	NullValue() {
		super();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key toKey() {
		return Keys.nullKey();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte[] toArray() {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final Value o) {
		// null      = 0
		// NullValue = 0
		// AnyValue  = -1
		// ByteValue = -1
		if (o == this || o == null || o instanceof NullValue) {
			return 0;
		}
		return -1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {
		// null      = true
		// NullValue = true
		// AnyValue  = false
		// ByteValue = false
		if (obj == null || obj instanceof NullValue) {
			return true;
		}
		return false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "NULL";
	}
}
