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

package net.dsys.tkvs.impl.transaction;

import java.nio.ByteBuffer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.impl.lang.FastArrays;
import net.dsys.tkvs.api.transaction.TID;

/**
 * Four-byte long transaction ID, stored in one int.
 * 
 * @author Ricardo Padilha
 */
final class TID4 implements TID {

	static final int LENGTH = Integer.SIZE / Byte.SIZE; // length in bytes

	private final int value; // bytes 0 to 3

	TID4(@Nonnull final byte[] array, @Nonnegative final int offset) {
		if (array == null) {
			throw new NullPointerException("array == null");
		}
		if (offset < 0 || offset + LENGTH > array.length) {
			throw new ArrayIndexOutOfBoundsException(offset);
		}
		this.value = FastArrays.getInt(array, offset);
	}

	TID4(@Nonnull final ByteBuffer buffer) {
		this.value = buffer.getInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int length() {
		return LENGTH;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte[] toArray() {
		final byte[] array = new byte[LENGTH];
		FastArrays.putInt(array, 0, value);
		return array;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean matches(final byte[] array, final int offset) {
		return value == FastArrays.getInt(array, offset);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final TID o) {
		if (o == null) {
			return 1;
		}
		if (o == this) {
			return 0;
		}
		if (o instanceof TID4) {
			final TID4 t = (TID4) o;
			return Integer.compare(value, t.value);
		}
		return TIDs.compare(this, o);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj instanceof TID4) {
			final TID4 t = (TID4) obj;
			return value == t.value;
		}
		if (obj instanceof TID) {
			return TIDs.compare(this, (TID) obj) == 0;
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return String.format("%08X", Integer.valueOf(value));
	}
}
