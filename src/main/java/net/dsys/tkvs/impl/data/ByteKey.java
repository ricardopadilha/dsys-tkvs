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
final class ByteKey implements Key {

	private static final int LONG_LENGTH = Long.SIZE / Byte.SIZE;
	private static final int INT_LENGTH = Integer.SIZE / Byte.SIZE;

	private static final long serialVersionUID = 7L;

	private final byte[] array;
	private final long longValue;
	private final int intValue;
	private int hashCode;

	ByteKey(final int value) {
		this.array = FastArrays.toArray(value);
		this.longValue = ((long) value) << Integer.SIZE;
		this.intValue = value;
	}

	ByteKey(final long value) {
		this.array = FastArrays.toArray(value);
		this.longValue = value;
		this.intValue = (int) (value >>> Integer.SIZE);
	}

	ByteKey(@Nonnull final byte[] array) {
		this(array, 0, array.length);
	}

	ByteKey(@Nonnull final byte[] array, @Nonnegative final int offset, @Nonnegative final int length) {
		if (array == null) {
			throw new NullPointerException("array == null");
		}
		this.array = new byte[length];
		FastArrays.arrayCopy(array, offset, this.array, 0, length);
		if (length >= INT_LENGTH) {
			this.intValue = FastArrays.getInt(array, 0);
		} else {
			this.intValue = 0;
		}
		if (length >= LONG_LENGTH) {
			this.longValue = FastArrays.getLong(array, 0);
		} else {
			this.longValue = ((long) intValue) << Integer.SIZE;
		}
		this.hashCode = 0;
	}

	ByteKey(@Nonnull final ByteBuffer in) {
		if (in == null) {
			throw new NullPointerException("in == null");
		}
		final int length = in.remaining();
		this.array = new byte[length];
		in.get(array);
		if (length >= INT_LENGTH) {
			this.intValue = FastArrays.getInt(array, 0);
		} else {
			this.intValue = 0;
		}
		if (length >= LONG_LENGTH) {
			this.longValue = FastArrays.getLong(array, 0);
		} else {
			this.longValue = ((long) intValue) << Integer.SIZE;
		}
		this.hashCode = 0;
	}

	@Nonnull
	byte[] array() {
		return array;
	}

	@Nonnegative
	int arrayLength() {
		return array.length;
	}

	int intValue() {
		return intValue;
	}

	long longValue() {
		return longValue;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Value toValue() {
		return new ByteValue(array.clone());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte[] toArray() {
		return array.clone();
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
		if (hashCode == 0) {
			hashCode = FastArrays.hashCode(array, 0, array.length);
		}
		return hashCode;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return FastArrays.toString(array, 0, array.length);
	}
}
