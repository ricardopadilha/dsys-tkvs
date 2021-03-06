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
 * This is an abstraction used to lock "last key" operations.
 * 
 * @author Ricardo Padilha
 */
final class LastKey implements Key {

	private static final long serialVersionUID = 4L;

	LastKey() {
		super();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Value toValue() {
		throw new UnsupportedOperationException();
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
	public int compareTo(final Key o) {
		return Keys.compare(this, o);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {
		return Keys.equals(this, obj);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return Integer.MAX_VALUE;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "LAST";
	}
}
