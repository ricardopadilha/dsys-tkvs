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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.dsys.commons.impl.lang.FastArrays;
import net.dsys.tkvs.api.data.Value;

/**
 * @author Ricardo Padilha
 */
public final class Values {

	private static final Value NULL_VALUE = new NullValue();
	private static final Value ANY_VALUE = new AnyValue();

	private static final byte[] NULL = new byte[0];
	private static final byte[] ANY = new byte[0];

	private Values() {
		return;
	}

	@Nonnull
	public static Value nullValue() {
		return NULL_VALUE;
	}

	@Nonnull
	public static Value anyValue() {
		return ANY_VALUE;
	}

	public static boolean isNull(@Nullable final Value value) {
		return (value == null || value == NULL_VALUE || value instanceof NullValue);
	}

	public static boolean isAny(@Nullable final Value value) {
		return value == ANY_VALUE || value instanceof AnyValue;
	}

	@Nonnull
	public static Value from(final int value) {
		return new ByteValue(FastArrays.toArray(value));
	}

	@Nonnull
	public static Value from(final long value) {
		return new ByteValue(FastArrays.toArray(value));
	}

	@Nonnull
	public static Value from(@Nonnull final byte[] array) {
		return new ByteValue(array);
	}

	@Nonnull
	public static Value from(@Nonnull final byte[] array, @Nonnegative final int offset,
			@Nonnegative final int length) {
		return new ByteValue(array, offset, length);
	}

	@Nonnull
	public static Value from(@Nonnull final ByteBuffer in) {
		return new ByteValue(in);
	}

	@Nonnull
	public static Value from(@Nonnull final Serializable obj) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (final ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			oos.writeObject(obj);
			oos.close();
			return new ByteValue(bos.toByteArray());
		} catch (final IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Nonnull
	public static <T extends Serializable> T to(@Nonnull final Value value) {
		try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(value.toArray()))) {
			@SuppressWarnings("unchecked")
			final T obj = (T) ois.readObject();
			return obj;
		} catch (final IOException | ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Nonnull
	static Value fromHexString(@Nonnull final String hexString) {
		return new ByteValue(hexToBytes(hexString));
	}

	public static int compare(@Nullable final Value left, @Nullable final Value right) {
		final int compare;
		if (isNull(left) && isNull(right)) {
			compare = 0;
		} else if (isNull(left)) {
			compare = -1;
		} else if (isNull(right)) {
			compare = 1;
		} else if (isAny(left) || isAny(right)) {
			compare = 0;
		} else {
			final byte[] leftArray = left.toArray();
			final byte[] rightArray = right.toArray();
			compare = FastArrays.compareArrays(leftArray, rightArray);
		}
		return compare;
	}

	public static boolean equals(@Nullable final Value left, @Nullable final Object right) {
		final boolean equals;
		if (right instanceof Value) {
			equals = (compare(left, (Value) right) == 0);
		} else {
			equals = false;
		}
		return equals;
	}

	@Nonnull
	private static byte[] toArray(final Value value) {
		final byte[] array;
		if (value == null || value instanceof NullValue) {
			array = NULL;
		} else if (value instanceof AnyValue) {
			array = ANY;
		} else {
			array = value.toArray();
		}
		return array;
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
