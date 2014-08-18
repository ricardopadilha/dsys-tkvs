/**
 * Copyright 2013 Ricardo Padilha
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.dsys.commons.api.exception.Bug;
import net.dsys.commons.impl.lang.FastArrays;
import net.dsys.tkvs.api.data.Key;

/**
 * @author Ricardo Padilha
 */
public final class Keys {

	private static final int LONG_LENGTH = Long.SIZE / Byte.SIZE;
	private static final int INT_LENGTH = Integer.SIZE / Byte.SIZE;

	private static final Key NULL_KEY = new NullKey();
	private static final Key ANY_KEY = new AnyKey();
	private static final Key FIRST_KEY = new FirstKey();
	private static final Key LAST_KEY = new LastKey();

	private Keys() {
		return;
	}

	@Nonnull
	public static Key nullKey() {
		return NULL_KEY;
	}

	@Nonnull
	public static Key anyKey() {
		return ANY_KEY;
	}

	@Nonnull
	public static Key firstKey() {
		return FIRST_KEY;
	}

	@Nonnull
	public static Key lastKey() {
		return LAST_KEY;
	}

	public static boolean isNull(@Nullable final Object key) {
		return key == null || key == NULL_KEY || key instanceof NullKey;
	}

	public static boolean isNull(@Nullable final Object... keys) {
		if (keys == null) {
			return true;
		}
		for (final Object key : keys) {
			if (isNull(key)) {
				return true;
			}
		}
		return false;
	}

	public static boolean isAny(@Nullable final Object key) {
		return key == ANY_KEY || key instanceof AnyKey;
	}

	public static boolean isAny(@Nullable final Object... keys) {
		if (keys == null) {
			return false;
		}
		for (final Object key : keys) {
			if (isAny(key)) {
				return true;
			}
		}
		return false;
	}

	public static boolean isFirst(@Nullable final Object key) {
		return key == FIRST_KEY || key instanceof FirstKey;
	}

	public static boolean isLast(@Nullable final Object key) {
		return key == LAST_KEY || key instanceof LastKey;
	}

	public static boolean isMeta(@Nullable final Object key) {
		return isFirst(key) || isLast(key);
	}

	@Nonnull
	public static Key from(final int value) {
		return new ByteKey(value);
	}

	@Nonnull
	public static Key from(final long value) {
		return new ByteKey(value);
	}

	@Nonnull
	public static Key from(@Nonnull final byte[] array) {
		return new ByteKey(array, 0, array.length);
	}

	@Nonnull
	public static Key from(@Nonnull final byte[] array, @Nonnegative final int offset,
			@Nonnegative final int length) {
		return new ByteKey(array, offset, length);
	}

	@Nonnull
	public static Key from(@Nonnull final ByteBuffer in) {
		return new ByteKey(in);
	}

	@Nonnull
	public static <T extends Serializable> Key from(@Nonnull final T obj) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (final ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			oos.writeObject(obj);
			oos.close();
			return new ByteKey(bos.toByteArray());
		} catch (final IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Nonnull
	public static Key join(@Nonnull final Serializable... objs) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (final ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			oos.writeObject(objs);
			oos.close();
			return new ByteKey(bos.toByteArray());
		} catch (final IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Nonnull
	public static <T extends Serializable> T to(@Nonnull final Key key) {
		try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(key.toArray()))) {
			@SuppressWarnings("unchecked")
			final T obj = (T) ois.readObject();
			return obj;
		} catch (final IOException | ClassNotFoundException | ClassCastException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Nonnull
	public static Serializable[] split(@Nonnull final Key key) {
		try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(key.toArray()))) {
			final Serializable[] obj = (Serializable[]) ois.readObject();
			return obj;
		} catch (final IOException | ClassNotFoundException | ClassCastException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Nonnull
	public static <T extends Serializable> T split(@Nonnull final Key key, @Nonnegative final int index) {
		try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(key.toArray()))) {
			final Serializable[] objs = (Serializable[]) ois.readObject();
			@SuppressWarnings("unchecked")
			final T obj = (T) objs[index];
			return obj;
		} catch (final IOException | ClassNotFoundException | ClassCastException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Nonnull
	static Key fromHexString(@Nonnull final String hexString) {
		return new ByteKey(hexToBytes(hexString));
	}

	public static int compare(@Nonnull final Key left, @Nullable final Key right) {
		final int compare;
		if (left == right) {
			compare = 0;
		} else if (isNull(left)) {
			compare = compareToNull(right);
		} else if (isNull(right)) {
			compare = compareToNull(left);
		} else if (isAny(left)) {
			compare = compareToAny(right);
		} else if (isAny(right)) {
			compare = compareToAny(left);
		} else if (isFirst(left)) {
			compare = compareToFirst(right);
		} else if (isFirst(right)) {
			compare = compareToFirst(left);
		} else if (isLast(left)) {
			compare = compareToLast(right);
		} else if (isLast(right)) {
			compare = compareToLast(left);
		} else if (left instanceof ByteKey && right instanceof ByteKey) {
			final ByteKey lk = (ByteKey) left;
			final ByteKey rk = (ByteKey) right;
			int cmp = unsignedCompare(lk.intValue(), rk.intValue());
			if (cmp == 0) {
				cmp = unsignedCompare(lk.longValue(), rk.longValue());
			}
			if (cmp == 0) {
				cmp = FastArrays.compareArrays(lk.array(), rk.array());
			}
			compare = cmp;
		} else {
			final byte[] leftArray = left.toValue().toArray();
			final byte[] rightArray = right.toValue().toArray();
			compare = FastArrays.compareArrays(leftArray, rightArray);
		}
		return compare;
	}

	public static boolean equals(@Nonnull final Key left, @Nullable final Object right) {
		final boolean equals;
		if (left == right) {
			equals = true;
		} else if (!(right instanceof Key)) {
			equals = false;
		} else if (isNull(left)) {
			equals = isNull(right);
		} else if (isNull(right)) {
			equals = isNull(left);
		} else if (isAny(left)) {
			equals = equalsToAny(right);
		} else if (isAny(right)) {
			equals = equalsToAny(left);
		} else if (isFirst(left)) {
			equals = equalsToFirst(right);
		} else if (isFirst(right)) {
			equals = equalsToFirst(left);
		} else if (isLast(left)) {
			equals = equalsToLast(right);
		} else if (isLast(right)) {
			equals = equalsToLast(left);
		} else if (left instanceof ByteKey && right instanceof ByteKey) {
			final ByteKey lk = (ByteKey) left;
			final ByteKey rk = (ByteKey) right;
			if (lk.intValue() != rk.intValue()) {
				equals = false;
			} else if (lk.longValue() != rk.longValue()) {
				equals = false;
			} else if ((lk.arrayLength() == INT_LENGTH)
					&& (rk.arrayLength() == INT_LENGTH)
					&& (lk.intValue() == rk.intValue())) {
				equals = true;
			} else if ((lk.arrayLength() == LONG_LENGTH)
					&& (rk.arrayLength() == LONG_LENGTH)
					&& (lk.longValue() == rk.longValue())) {
				equals = true;
			} else {
				equals = (FastArrays.compareArrays(lk.array(), rk.array()) == 0);
			}
		} else {
			final byte[] leftArray = left.toValue().toArray();
			final byte[] rightArray = ((Key) right).toValue().toArray();
			equals = (FastArrays.compareArrays(leftArray, rightArray) == 0);
		}
		return equals;
	}

	public static void updateChecksum(@Nonnull final Key key, @Nonnull final Checksum checksum) {
		if (key instanceof ByteKey) {
			final ByteKey bk = (ByteKey) key;
			final byte[] array = bk.array();
			checksum.update(array, 0, array.length);
		} else {
			throw new Bug("cannot checksum meta key: " + key);
		}
	}

	private static int compareToNull(@Nullable final Key right) {
		// null     = 0
		// NullKey  = 0
		// AnyKey   = -1
		// FirstKey = -1
		// LastKey  = -1
		// IntKey   = -1
		// ByteKey  = -1
		final int compare;
		if (isNull(right)) {
			compare = 0;
		} else {
			compare = -1;
		}
		return compare;
	}

	private static boolean equalsToAny(@Nullable final Object right) {
		// null     = false
		// NullKey  = false
		// AnyKey   = true
		// FirstKey = true
		// LastKey  = true
		// IntKey   = true
		// LongKey  = true
		// ByteKey  = true
		final boolean equals;
		if (isNull(right)) {
			equals = false;
		} else {
			equals = true;
		}
		return equals;
	}

	private static int compareToAny(@Nullable final Key right) {
		// null     = 1
		// NullKey  = 1
		// AnyKey   = 0
		// FirstKey = -1
		// LastKey  = -1
		// IntKey   = -1
		// ByteKey  = -1
		final int compare;
		if (isNull(right)) {
			compare = 1;
		} else if (isAny(right)) {
			compare = 0;
		} else {
			compare = -1;
		}
		return compare;
	}

	private static boolean equalsToFirst(@Nullable final Object right) {
		// null     = false
		// NullKey  = false
		// AnyKey   = true
		// FirstKey = true
		// LastKey  = false
		// IntKey   = false
		// LongKey  = false
		// ByteKey  = false
		final boolean equals;
		if (isAny(right) || isFirst(right)) {
			equals = true;
		} else {
			equals = false;
		}
		return equals;
	}

	private static int compareToFirst(@Nullable final Key right) {
		// null     = 1
		// NullKey  = 1
		// AnyKey   = 0
		// FirstKey = 0
		// LastKey  = -1
		// IntKey   = -1
		// LongKey  = -1
		// ByteKey  = -1
		final int compare;
		if (isAny(right) || isFirst(right)) {
			compare = 0;
		} else if (isNull(right)) {
			compare =  1;
		} else {
			compare = -1;
		}
		return compare;
	}

	private static boolean equalsToLast(@Nullable final Object right) {
		// null     = false
		// NullKey  = false
		// AnyKey   = true
		// FirstKey = false
		// LastKey  = true
		// IntKey   = false
		// LongKey  = false
		// ByteKey  = false
		final boolean equals;
		if (isAny(right) || isLast(right)) {
			equals = true;
		} else {
			equals = false;
		}
		return equals;
	}

	private static int compareToLast(@Nullable final Key right) {
		// null     = 1
		// NullKey  = 1
		// AnyKey   = 0
		// FirstKey = 1
		// LastKey  = 0
		// IntKey   = 1
		// LongKey  = 1
		// ByteKey  = 1
		final int compare;
		if (isAny(right) || isLast(right)) {
			compare = 0;
		} else {
			compare = 1;
		}
		return compare;
	}

	private static int unsignedCompare(final long left, final long right) {
		final int compare;
		if (left == right) {
			compare = 0;
		} else if ((left < right) ^ (left < 0) ^ (right < 0)) {
			compare = -1;
		} else {
			compare = 1;
		}
		return compare;
	}

	@Nonnull
	private static byte[] hexToBytes(@Nonnull final String hexString) {
		if (hexString == null) {
			throw new NullPointerException("hexString == null");
		}
		if ((hexString.length() % 2) != 0) {
			throw new IllegalArgumentException("(hexString.length() % 2) != 0");
		}
		final int len = hexString.length() / 2;
		final byte[] array = new byte[len];
		for (int i = 0; i < len; i++) {
			final int index = i * 2;
			final String byteElement = hexString.substring(index, index + 2);
			array[i] = (byte) Integer.parseInt(byteElement, 16);
		}
		return array;
	}
}
