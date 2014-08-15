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
final class TID16 implements TID {

	static final int LENGTH = 2 * Long.SIZE / Byte.SIZE; // length in bytes
	private static final int LONG_LENGTH = Long.SIZE / Byte.SIZE;

	private final long long0; // bytes 0 to 7
	private final long long1; // bytes 8 to 15
	private final int hashCode;

	TID16(@Nonnull final byte[] array, @Nonnegative final int offset) {
		if (array == null) {
			throw new NullPointerException("array == null");
		}
		if (offset < 0 || offset + LENGTH > array.length) {
			throw new ArrayIndexOutOfBoundsException(offset);
		}
		this.long0 = FastArrays.getLong(array, offset);
		this.long1 = FastArrays.getLong(array, offset + LONG_LENGTH);
		final int hash0 = (int) (long0 ^ (long0 >>> 32));
		final int hash1 = (int) (long1 ^ (long1 >>> 32));
		this.hashCode = 31 * hash0 + hash1;
	}

	TID16(@Nonnull final ByteBuffer buffer) {
		this.long0 = buffer.getLong();
		this.long1 = buffer.getLong();
		final int hash0 = (int) (long0 ^ (long0 >>> 32));
		final int hash1 = (int) (long1 ^ (long1 >>> 32));
		this.hashCode = 31 * hash0 + hash1;
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
		FastArrays.putLong(array, 0, long0);
		FastArrays.putLong(array, LONG_LENGTH, long1);
		return array;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean matches(final byte[] array, final int offset) {
		return long0 == FastArrays.getLong(array, offset)
				&& long1 == FastArrays.getLong(array, offset + LONG_LENGTH);
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
		if (o instanceof TID16) {
			final TID16 t = (TID16) o;
			if (long0 != t.long0) {
				return Long.compare(long0, t.long0);
			}
			if (long1 != t.long1) {
				return Long.compare(long1, t.long1);
			}
			return 0;
		}
		return TIDs.compare(this, o);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return hashCode;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj instanceof TID16) {
			final TID16 t = (TID16) obj;
			return long0 == t.long0 && long1 == t.long1;
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
		return String.format("%016X%016X", Long.valueOf(long0), Long.valueOf(long1));
	}
}
