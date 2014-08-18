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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.dsys.commons.impl.lang.FastArrays;
import net.dsys.tkvs.api.data.Value;
import net.dsys.tkvs.impl.data.Values;

/**
 * @author Ricardo Padilha
 */
public final class Operators {

	private static final int MASK = 0xFF;

	private Operators() {
		// no instantiation
	}

	/**
	 * Increments a value as if it were an unsigned number.
	 * 
	 * @return a new instance of Value, incremented
	 * @throws ArithmeticException
	 *             if value could not be incremented, i.e., all bytes are
	 *             already at their maximum unsigned value.
	 */
	@Nonnull
	public static Value increment(@Nonnull final Value value) {
		final byte[] array = value.toArray().clone();
		final int len = array.length;
		for (int i = len - 1; i >= 0; i--) {
			int v = array[i] & 0xFF;
			if (v == 255) {
				// overflow, we carry the sum
				array[i] = 0;
				continue;
			}
			assert v < 255;
			array[i] = (byte) ++v;
			return Values.from(array);
		}
		throw new ArithmeticException("cannot increment: value overflow");
	}

	/**
	 * Decrements a value as if it were an unsigned number.
	 * 
	 * @return a new instance of Value, decremented
	 * @throws ArithmeticException
	 *             if value could not be Decremented, i.e., all bytes are
	 *             already at their minimum unsigned value.
	 */
	@Nonnull
	public static Value decrement(@Nonnull final Value value) {
		final byte[] array = value.toArray().clone();
		final int len = array.length;
		for (int i = len - 1; i >= 0; i--) {
			int v = array[i] & 0xFF;
			if (v == 0) {
				// underflow, we carry the subtraction
				array[i] = (byte) 255;
				continue;
			}
			assert v > 0;
			array[i] = (byte) --v;
			return Values.from(array);
		}
		throw new ArithmeticException("cannot decrement: value underflow");
	}

	/**
	 * Adds two values together as if they were two unsigned BigIntegers.
	 * If there is not enough space, the byte array was expanded.
	 */
	@Nonnull
	public static Value add(@Nonnull final Value a, @Nonnull final Value b) {
		final byte[] aa = a.toArray();
		final byte[] bb = b.toArray();
		// make sure the left array is longer
		final byte[] left;
		final byte[] right;
		if (aa.length >= bb.length) {
			left = aa;
			right = bb;
		} else { // aa.length < bb.length
			left = bb;
			right = aa;
		}
		return Values.from(add(left, right));
	}

	/**
	 * Assumption: left.length >= right.length
	 * 
	 * @param left
	 *            not modified
	 * @param right
	 *            not modified
	 * @return
	 */
	@Nonnull
	private static byte[] add(@Nonnull final byte[] left, @Nonnull final byte[] right) {
		int leftIndex = left.length;
		int rightIndex = right.length;
		final byte[] result = new byte[leftIndex];
		int sum = 0;

		while (rightIndex > 0) {
			sum = (left[--leftIndex] & MASK) + (right[--rightIndex] & MASK) + (sum >>> Byte.SIZE);
			result[leftIndex] = (byte) sum;
		}

		boolean carry = (sum >>> Byte.SIZE != 0);
		while (leftIndex > 0 && carry) {
			carry = ((result[--leftIndex] = (byte) (left[leftIndex] + 1)) == 0);
		}

		while (leftIndex > 0) {
			result[--leftIndex] = left[leftIndex];
		}

		if (carry) {
			final byte[] carried = new byte[result.length + 1];
			System.arraycopy(result, 0, carried, 1, result.length);
			carried[0] = 1;
			return carried;
		}
		return result;
	}

	@Nonnull
	public static Value subtract(@Nonnull final Value a, @Nonnull final Value b) {
		return null;
	}

	/**
	 * Assumption: left.length >= right.length
	 * 
	 * @param left
	 *            not modified
	 * @param right
	 *            not modified
	 * @return
	 */
	@Nonnull
	private static byte[] subtract(@Nonnull final byte[] left, @Nonnull final byte[] right) {
		int leftIndex = left.length;
		int rightIndex = right.length;
		final byte[] result = new byte[leftIndex];
		int sum = 0;

		while (rightIndex > 0) {
			sum = (left[--leftIndex] & MASK) + (right[--rightIndex] & MASK) + (sum >>> Byte.SIZE);
			result[leftIndex] = (byte) sum;
		}

		boolean carry = (sum >>> Byte.SIZE != 0);
		while (leftIndex > 0 && carry) {
			carry = ((result[--leftIndex] = (byte) (left[leftIndex] + 1)) == 0);
		}

		while (leftIndex > 0) {
			result[--leftIndex] = left[leftIndex];
		}

		if (carry) {
			throw new ArithmeticException("cannot add: value overflow");
		}
		return result;
	}

	@Nonnull
	public static Value multiply(@Nonnull final Value a, @Nonnull final Value b) {
		return null;
	}

	@Nonnull
	public static Value divide(@Nonnull final Value a, @Nonnull final Value b) {
		return null;
	}

	@Nonnull
	public static Value concatenate(@Nonnull final Value... values) {
		final List<byte[]> arrays = new ArrayList<>(values.length);
		int length = 0;
		for (final Value value : values) {
			final byte[] array = value.toArray();
			length += array.length;
		}
		final byte[] concatenation = new byte[length];
		int position = 0;
		for (final byte[] array : arrays) {
			final int len = array.length;
			FastArrays.arrayCopy(array, 0, concatenation, position, len);
			position += len;
		}
		return Values.from(concatenation);
	}

	public static boolean lessThan(@Nullable final Value a, @Nullable final Value b) {
		return Values.compare(a, b) < 0;
	}

	public static boolean lessEqual(@Nullable final Value a, @Nullable final Value b) {
		return Values.compare(a, b) <= 0;
	}

	public static boolean moreThan(@Nullable final Value a, @Nullable final Value b) {
		return Values.compare(a, b) > 0;
	}

	public static boolean moreEqual(@Nullable final Value a, @Nullable final Value b) {
		return Values.compare(a, b) >= 0;
	}

	public static boolean equals(@Nullable final Value a, @Nullable final Value b) {
		return Values.compare(a, b) == 0;
	}

}
