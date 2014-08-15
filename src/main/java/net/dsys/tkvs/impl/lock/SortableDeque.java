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

package net.dsys.tkvs.impl.lock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author Ricardo Padilha
 */
final class SortableDeque<E> {

	@Nonnull
	private final Function<E, E> identity;
	@Nonnull
	private final IteratorCurrent<E> empty;
	@Nonnull
	private E[] data;
	@Nonnegative
	private int mask;
	@Nonnegative
	private int head;
	@Nonnegative
	private int tail;

	SortableDeque(@Nonnegative final int capacity) {
		this.identity = createIdentity();
		this.empty = createEmptyIterator();
		this.data = allocateElements(capacity);
		this.mask = data.length - 1;
		this.head = 0;
		this.tail = 0;
	}

	void pushLast(@Nonnull final E e) {
		if (e == null) {
			throw new NullPointerException();
		}
		data[tail] = e;
		tail = inc(tail);
		if (tail == head) {
			doubleCapacity();
		}
	}

	@Nullable
	E pollFirst() {
		final E result = data[head];
		if (result == null) {
			return null;
		}
		data[head] = null;
		head = inc(head);
		return result;
	}

	@Nullable
	E peekFirst() {
		return data[head];
	}

	@Nullable
	E pollLast() {
		final int i = dec(tail);
		final E result = data[i];
		if (result == null) {
			return null;
		}
		data[i] = null;
		tail = i;
		return result;
	}

	@Nullable
	E peekLast() {
		return data[dec(tail)];
	}

	@Nullable
	<V> E getPrevious(@Nonnull final Function<E, V> f, @Nonnull final V from) {
		if (from == null) {
			return null;
		}
		E x;
		for (int i = dec(tail); (x = data[i]) != null; i = dec(i)) {
			if (f.apply(x).equals(from)) {
				return data[(i - 1) & mask];
			}
		}
		return null;
	}

	@Nonnull
	IteratorCurrent<E> iterator() {
		if (isEmpty()) {
			return empty;
		}
		return createIterator(head, identity);
	}

	@Nonnull
	IteratorCurrent<E> iterator(@Nonnull final E from) {
		if (from == null) {
			return null;
		}
		E x;
		for (int i = head; (x = data[i]) != null; i = inc(i)) {
			if (x.equals(from)) {
				return createIterator(i, identity);
			}
		}
		return empty;
	}

	@Nonnull
	<V> IteratorCurrent<V> iterator(@Nonnull final E from, @Nonnull final Function<E, V> f) {
		if (from == null) {
			return null;
		}
		E x;
		for (int i = head; (x = data[i]) != null; i = inc(i)) {
			if (x.equals(from)) {
				return createIterator(i, f);
			}
		}
		return createEmptyIterator();
	}

	@SuppressWarnings("unchecked")
	void sort() {
		if (head > tail) {
			final int capacity = data.length;
			final Object[] newData = new Object[capacity];
			final int right = capacity - head;
			// [0 ... tail ... head ... capacity]
			// copy head->capacity to 0->right
			System.arraycopy(data, head, newData, 0, right);
			// copy 0->tail to right->length
			System.arraycopy(data, 0, newData, right, tail);
			data = (E[]) newData;
			head = 0;
			tail += right;
		}
		Arrays.sort(data, head, tail);
	}

	@Nonnegative
	int size() {
		return (tail - head) & mask;
	}

	boolean isEmpty() {
		return head == tail;
	}

	void clear() {
		if (head < tail) {
			Arrays.fill(data, head, tail, null);
		} else if (head > tail) {
			Arrays.fill(data, 0, tail, null);
			Arrays.fill(data, head, data.length, null);
		}
		head = 0;
		tail = 0;
	}

	boolean delete(@Nonnegative final int i) {
		final int front = (i - head) & mask;
		final int back = (tail - i) & mask;

		// Optimize for least element motion
		if (front < back) {
			if (head <= i) {
				System.arraycopy(data, head, data, head + 1, front);
			} else { // i < head
				System.arraycopy(data, 0, data, 1, i);
				data[0] = data[mask];
				System.arraycopy(data, head, data, head + 1, mask - head);
			}
			data[head] = null;
			head = (head + 1) & mask;
			return false;
		}
		if (i < tail) { // Copy the null tail as well
			System.arraycopy(data, i + 1, data, i, back);
			tail = (tail - 1) & mask;
		} else { // tail <= i
			System.arraycopy(data, i + 1, data, i, mask - i);
			data[mask] = data[0];
			System.arraycopy(data, 1, data, 0, tail);
			tail = (tail - 1) & mask;
		}
		return true;
	}

	@Nonnull
	<V> List<V> toList(@Nonnull final Function<E, V> f) {
		final List<V> list = new ArrayList<>(size());
		E x;
		for (int i = head; (x = data[i]) != null; i = inc(i)) {
			list.add(f.apply(x));
		}
		return list;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append('[');
		if (data[head] != null) {
			sb.append(data[head]);
		}
		E x;
		for (int i = (head + 1) & mask; (x = data[i]) != null; i = inc(i)) {
			sb.append(',').append(' ').append(x);
		}
		sb.append(']');
		return sb.toString();
	}

	/**
	 * Increments the input value in a modulo fashion.
	 */
	@Nonnegative
	private int inc(@Nonnegative final int i) {
		return (i + 1) & mask;
	}

	/**
	 * Decrements the input value in a modulo fashion.
	 */
	@Nonnegative
	private int dec(@Nonnegative final int i) {
		return (i - 1) & mask;
	}

	@Nonnull
	private <V> IteratorCurrent<V> createIterator(@Nonnegative final int start,
			@Nonnull final Function<E, V> f) {
		final int mask = this.mask;
		final E[] data = this.data;

		return new IteratorCurrent<V>() {
			private int i = dec(start);
			@Override public V current() {
				return f.apply(data[i]);
			}
			@Override public boolean hasNext() {
				return data[inc(i)] != null;
			}
			@Override public V next() {
				i = inc(i);
				if (data[i] == null) {
					throw new NoSuchElementException();
				}
				return f.apply(data[i]);
			}
			@Override public void remove() {
				if (delete(i)) {
					i = dec(i);
				}
			}
			private int inc(final int i) {
				return (i + 1) & mask;
			}
			private int dec(final int i) {
				return (i - 1) & mask;
			}
		};
	}

	@Nonnull
	private static <T> IteratorCurrent<T> createEmptyIterator() {
		return new IteratorCurrent<T>() {
			@Override public T current() { return null; }
			@Override public boolean hasNext() { return false; }
			@Override public T next() { throw new NoSuchElementException(); }
			@Override public void remove() { return; }
		};
	}

	@Nonnull
	private static <T> Function<T, T> createIdentity() {
		return new Function<T, T>() {
			@Override
			public T apply(final T input) {
				return input;
			}
		};
	}

	@Nonnull
	private static <E> E[] allocateElements(@Nonnegative final int numElements) {
		final int capacity;
		if (Integer.bitCount(numElements) == 1) {
			capacity = numElements;
		} else {
			final int temp = Integer.highestOneBit(numElements) << 1;
			if (temp < 0) {
				// larger than MAX_INTEGER
				capacity = temp >>> 1;
			} else {
				capacity = temp;
			}
		}
		@SuppressWarnings("unchecked")
		final E[] array = (E[]) new Object[capacity];
		return array;
	}

	@SuppressWarnings("unchecked")
	private void doubleCapacity() {
		assert head == tail;
		final int capacity = data.length;
		final int newCapacity = capacity << 1;
		if (newCapacity < 0) {
			throw new IllegalStateException("deque too big");
		}
		final Object[] newData = new Object[newCapacity];
		final int right = capacity - head;
		System.arraycopy(data, head, newData, 0, right);
		System.arraycopy(data, 0, newData, right, head);
		data = (E[]) newData;
		mask = newCapacity - 1;
		head = 0;
		tail = capacity;
	}
}
