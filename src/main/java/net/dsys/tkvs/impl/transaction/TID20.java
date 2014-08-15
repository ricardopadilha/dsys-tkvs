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
 * Twenty-byte long transaction ID, stored in ints.
 * 
 * @author Ricardo Padilha
 */
final class TID20 implements TID {

	static final int LENGTH = 5 * Integer.SIZE / Byte.SIZE; // length in bytes
	private static final int INT_LENGTH = Integer.SIZE / Byte.SIZE;

	private final int int0; // bytes 0 to 3
	private final int int1; // bytes 4 to 7
	private final int int2; // bytes 8 to 11
	private final int int3; // bytes 12 to 15
	private final int int4; // bytes 16 to 19
	private final int hashCode;

	TID20(@Nonnull final byte[] array, @Nonnegative final int offset) {
		if (array == null) {
			throw new NullPointerException("array == null");
		}
		if (offset < 0 || offset + LENGTH > array.length) {
			throw new ArrayIndexOutOfBoundsException(offset);
		}
		int pos = offset;
		this.int0 = FastArrays.getInt(array, pos);
		pos += INT_LENGTH;
		this.int1 = FastArrays.getInt(array, pos);
		pos += INT_LENGTH;
		this.int2 = FastArrays.getInt(array, pos);
		pos += INT_LENGTH;
		this.int3 = FastArrays.getInt(array, pos);
		pos += INT_LENGTH;
		this.int4 = FastArrays.getInt(array, pos);
		this.hashCode = (31 * (31 * (31 * (31 * (int0) + int1) + int2) + int3) + int4);
	}

	TID20(@Nonnull final ByteBuffer buffer) {
		this.int0 = buffer.getInt();
		this.int1 = buffer.getInt();
		this.int2 = buffer.getInt();
		this.int3 = buffer.getInt();
		this.int4 = buffer.getInt();
		this.hashCode = (31 * (31 * (31 * (31 * (int0) + int1) + int2) + int3) + int4);
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
		int offset = 0;
		FastArrays.putInt(array, offset, int0);
		offset += INT_LENGTH;
		FastArrays.putInt(array, offset, int1);
		offset += INT_LENGTH;
		FastArrays.putInt(array, offset, int2);
		offset += INT_LENGTH;
		FastArrays.putInt(array, offset, int3);
		offset += INT_LENGTH;
		FastArrays.putInt(array, offset, int4);
		return array;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean matches(final byte[] array, final int offset) {
		int pos = offset;
		int i;
		i = FastArrays.getInt(array, pos);
		if (int0 != i) {
			return false;
		}
		pos += INT_LENGTH;
		i = FastArrays.getInt(array, pos);
		if (int1 != i) {
			return false;
		}
		pos += INT_LENGTH;
		i = FastArrays.getInt(array, pos);
		if (int2 != i) {
			return false;
		}
		pos += INT_LENGTH;
		i = FastArrays.getInt(array, pos);
		if (int3 != i) {
			return false;
		}
		pos += INT_LENGTH;
		i = FastArrays.getInt(array, pos);
		if (int4 != i) {
			return false;
		}
		return true;
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
		if (o instanceof TID20) {
			final TID20 t = (TID20) o;
			if (int0 != t.int0) {
				return Integer.compare(int0, t.int0);
			}
			if (int1 != t.int1) {
				return Integer.compare(int1, t.int1);
			}
			if (int2 != t.int2) {
				return Integer.compare(int2, t.int2);
			}
			if (int3 != t.int3) {
				return Integer.compare(int3, t.int3);
			}
			if (int4 != t.int4) {
				return Integer.compare(int4, t.int4);
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
		if (obj instanceof TID20) {
			final TID20 t = (TID20) obj;
			return int0 == t.int0
				&& int1 == t.int1
				&& int2 == t.int2
				&& int3 == t.int3
				&& int4 == t.int4;
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
		return String.format("%08X%08X%08X%08X%08X",
				Integer.valueOf(int0),
				Integer.valueOf(int1),
				Integer.valueOf(int2),
				Integer.valueOf(int3),
				Integer.valueOf(int4));
		//return String.format("%08X", Integer.valueOf(int4));
	}
}
