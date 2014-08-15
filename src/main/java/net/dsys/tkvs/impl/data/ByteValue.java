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

import java.nio.ByteBuffer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.impl.lang.FastArrays;
import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.data.Value;

/**
 * @author Ricardo Padilha
 */
final class ByteValue implements Value {

	private static final long serialVersionUID = 7L;

	private final byte[] array;

	ByteValue(@Nonnull final byte[] array) {
		if (array == null) {
			throw new NullPointerException("array == null");
		}
		this.array = array;
	}
	
	ByteValue(@Nonnull final byte[] array, @Nonnegative final int offset, @Nonnegative final int length) {
		if (array == null) {
			throw new NullPointerException("array == null");
		}
		this.array = new byte[length];
		FastArrays.arrayCopy(array, offset, this.array, 0, length);
	}

	ByteValue(@Nonnull final ByteBuffer in) {
		if (in == null) {
			throw new NullPointerException("in == null");
		}
		final int length = in.remaining();
		this.array = new byte[length];
		in.get(array);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key toKey() {
		return new ByteKey(array);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte[] toArray() {
		return array;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final Value o) {
		return Values.compare(this, o);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {
		return Values.equals(this, obj);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return FastArrays.hashCode(array);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return FastArrays.toString(array, 0, array.length);
	}

}
