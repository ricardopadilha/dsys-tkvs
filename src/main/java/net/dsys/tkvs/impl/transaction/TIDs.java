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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.dsys.commons.impl.lang.FastArrays;
import net.dsys.tkvs.api.transaction.TID;

/**
 * @author Ricardo Padilha
 */
public final class TIDs {

	private TIDs() {
		// no instantiation
	}

	@Nonnull
	public static TID getTID(@Nonnull final String hexString) {
		final byte[] array = hexToBytes(hexString);
		return getTID(array);
	}

	@Nonnull
	public static TID getTID(@Nonnull final byte[] array) {
		switch (array.length) {
			case TID4.LENGTH: {
				return new TID4(array, 0);
			}
			case TID8.LENGTH: {
				return new TID8(array, 0);
			}
			case TID16.LENGTH: {
				return new TID16(array, 0);
			}
			case TID20.LENGTH: {
				return new TID20(array, 0);
			}
			default: {
				throw new IllegalArgumentException("unsupported array length: " + array.length);
			}
		}
	}

	@Nonnull
	public static TID getTID4(@Nonnull final ByteBuffer buffer) {
		return new TID4(buffer);
	}

	@Nonnull
	public static TID getTID8(@Nonnull final ByteBuffer buffer) {
		return new TID8(buffer);
	}

	@Nonnull
	public static TID getTID16(@Nonnull final ByteBuffer buffer) {
		return new TID16(buffer);
	}

	@Nonnull
	public static TID getTID20(@Nonnull final ByteBuffer buffer) {
		return new TID20(buffer);
	}

	public static int compare(@Nullable final TID left, @Nullable final TID right) {
		final int compare;
		if (left == null && right == null) {
			compare = 0;
		} else if (left == null) {
			compare = -1;
		} else if (right == null) {
			compare = 1;
		} else {
			final byte[] l = left.toArray();
			final byte[] r = right.toArray();
			compare = FastArrays.compareArrays(l, r);
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
