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

import java.util.NoSuchElementException;
import java.util.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @param <K>
 *            the type of keys maintained by this map
 * @param <V>
 *            the type of mapped values
 * 
 * @author Ricardo Padilha
 */
final class IntervalTreeMap<K extends Comparable<K>, V extends Comparable<V>> {

	private static final Color BLACK = Color.BLACK;
	private static final Color RED = Color.RED;

	@Nonnull
	private final IteratorCurrent<V> empty;
	@Nullable
	private Entry<K, V> root;
	@Nonnegative
	private int size;

	IntervalTreeMap() {
		this.empty = createEmptyIterator();
		this.root = null;
		this.size = 0;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append('{');
		Entry<K, V> e = first();
		while (e != null) {
			sb.append(String.format("[%s;%s]=%s", e.getStart(), e.getEnd(), e.getValue()));
			e = successor(e);
			if (e != null) {
				sb.append(", ");
			}
		}
		sb.append('}');
		return sb.toString();
	}

	boolean isEmpty() {
		return size == 0;
	}

	@Nonnegative
	int size() {
		return size;
	}

	@Nullable
	V get(@Nonnull final K key) {
		return get(key, key);
	}

	@Nullable
	V get(@Nonnull final K start, @Nonnull final K end) {
		Entry<K, V> p = root;
		while (p != null && !p.intersects(start, end)) {
			if (p.getLeft() != null && p.getLeft().childrenIntersects(start, end)) {
				p = p.getLeft();
			} else {
				p = p.getRight();
			}
		}
		if (p != null) {
			return p.getValue();
		}
		return null;
	}

	@Nullable
	V getFirst(@Nonnull final K start, @Nonnull final K end) {
		Entry<K, V> p = root;
		while (p != null) {
			if (p.getLeft() != null
					&& p.getLeft().childrenIntersects(start, end)
					&& (p.getRight() == null
						|| p.getLeft().getMinStart().compareTo(p.getRight().getMinStart()) == 0
						|| p.getRight().getMinStart().compareTo(start) > 0)) {
				p = p.getLeft();
			} else if (p.getRight() != null && !p.intersects(start, end)
					&& p.getRight().childrenIntersects(start, end)) {
				p = p.getRight();
			} else {
				break;
			}
		}
		if (p != null && p.intersects(start, end)) {
			return p.getValue();
		}
		return null;
	}

	@Nullable
	V getPrevious(@Nonnull final K start, @Nonnull final K end, @Nonnull final V value) {
		final Entry<K, V> e = find(start, end, value);
		final Entry<K, V> prev = predecessor(e);
		if (prev != null && prev.matches(start, end)) {
			return prev.getValue();
		}
		return null;
	}

	@Nullable
	V getLast(@Nonnull final K start, @Nonnull final K end) {
		Entry<K, V> p = root;
		while (p != null) {
			if (p.getRight() != null && p.getRight().childrenIntersects(start, end)) {
				p = p.getRight();
			} else if (p.getLeft() != null && !p.intersects(start, end) && p.getLeft().childrenIntersects(start, end)) {
				p = p.getLeft();
			} else {
				break;
			}
		}
		if (p != null && p.intersects(start, end)) {
			return p.getValue();
		}
		return null;
	}

	void getAll(@Nonnull final K start, @Nonnull final K end, @Nonnull final SortableDeque<Tuple<K, V>> results) {
		collect(root, start, end, results);
	}

	@Nonnull
	IteratorCurrent<V> iterator() {
		if (isEmpty()) {
			return empty;
		}
		return createIterator(first());
	}

	@Nonnull
	IteratorCurrent<V> iterator(@Nonnull final K start, @Nonnull final K end, @Nonnull final V value) {
		if (start == null || end == null || value == null) {
			throw new NullPointerException();
		}
		final Entry<K, V> e = find(start, end, value);
		if (e != null) {
			return createIterator(e);
		}
		return empty;
	}

	boolean put(@Nonnull final K start, @Nonnull final K end, @Nonnull final V value) {
		if (start == null || end == null || value == null) {
			throw new NullPointerException();
		}

		Entry<K, V> t = root;
		if (t == null) {
			root = new Entry<>(start, end, value, null);
			size = 1;
			return true;
		}

		int cmp;
		Entry<K, V> parent;
		do {
			parent = t;
			cmp = start.compareTo(t.getStart());
			if (cmp < 0) {
				t = t.getLeft();
				continue;
			} else if (cmp > 0) {
				t = t.getRight();
				continue;
			}
			cmp = end.compareTo(t.getEnd());
			if (cmp < 0) {
				t = t.getLeft();
				continue;
			} else if (cmp > 0) {
				t = t.getRight();
				continue;
			}
			cmp = value.compareTo(t.getValue());
			if (cmp < 0) {
				t = t.getLeft();
				continue;
			} else if (cmp > 0) {
				t = t.getRight();
				continue;
			} else {
				// everything is equal!
				return false;
			}
		} while (t != null);

		final Entry<K, V> e = new Entry<>(start, end, value, parent);
		if (cmp < 0) {
			parent.setLeft(e);
		} else {
			parent.setRight(e);
		}
		fixAfterInsertion(e);
		fixMaxRecursive(e);
		size++;
		return true;
	}

	boolean remove(@Nonnull final K start, @Nonnull final K end, @Nonnull final V value) {
		Entry<K, V> p = find(start, end, value);
		if (p == null) {
			return false;
		}

		// If strictly internal, copy successor's element to p and then make p
		// point to successor.
		if (p.getLeft() != null && p.getRight() != null) {
			final Entry<K, V> s = successor(p);
			p.setStart(s.getStart());
			p.setEnd(s.getEnd());
			p.setMaxEnd(s.getMaxEnd());
			p.setValue(s.getValue());
			p = s;
		} // p has 2 children

		final Entry<K, V> parent = p.getParent();
		// Start fixup at replacement node, if it exists.
		final Entry<K, V> replacement;
		if (p.getLeft() != null) {
			replacement = p.getLeft();
		} else {
			replacement = p.getRight();
		}

		if (replacement != null) {
			// Link replacement to parent
			replacement.setParent(p.getParent());
			if (p.getParent() == null) {
				root = replacement;
			} else if (p == p.getParent().getLeft()) {
				p.getParent().setLeft(replacement);
			} else {
				p.getParent().setRight(replacement);
			}

			// Null out links so they are OK to use by fixAfterDeletion.
			p.setLeft(null);
			p.setRight(null);
			p.setParent(null);

			// Fix replacement
			if (p.getColor() == BLACK) {
				fixAfterDeletion(replacement);
			}
		} else if (p.getParent() == null) { // return if we are the only node.
			root = null;
		} else { // No children. Use self as phantom replacement and unlink.
			if (p.getColor() == BLACK) {
				fixAfterDeletion(p);
			}

			if (p.getParent() != null) {
				if (p == p.getParent().getLeft()) {
					p.getParent().setLeft(null);
				} else if (p == p.getParent().getRight()) {
					p.getParent().setRight(null);
				}
				p.setParent(null);
			}
		}
		fixMaxRecursive(parent);
		size--;
		return true;
	}

	/**
	 * Removes all of the mappings from this map. The map will be empty after
	 * this call returns.
	 */
	void clear() {
		size = 0;
		root = null;
	}

	@Nullable
	private Entry<K, V> first() {
		Entry<K, V> p = root;
		if (p != null) {
			while (p.getLeft() != null) {
				p = p.getLeft();
			}
		}
		return p;
	}

	/**
	 * Returns the successor of the specified Entry, or null if no such.
	 */
	@Nullable
	static <K extends Comparable<K>, V extends Comparable<V>> Entry<K, V> successor(@Nullable final Entry<K, V> t) {
		if (t == null) {
			return null;
		} else if (t.getRight() != null) {
			Entry<K, V> p = t.getRight();
			while (p.getLeft() != null) {
				p = p.getLeft();
			}
			return p;
		} else {
			Entry<K, V> p = t.getParent();
			Entry<K, V> ch = t;
			while (p != null && ch == p.getRight()) {
				ch = p;
				p = p.getParent();
			}
			return p;
		}
	}

	/**
	 * Returns the predecessor of the specified Entry, or null if no such.
	 */
	@Nullable
	static <K extends Comparable<K>, V extends Comparable<V>> Entry<K, V> predecessor(@Nullable final Entry<K, V> t) {
		if (t == null) {
			return null;
		} else if (t.getLeft() != null) {
			Entry<K, V> p = t.getLeft();
			while (p.getRight() != null) {
				p = p.getRight();
			}
			return p;
		} else {
			Entry<K, V> p = t.getParent();
			Entry<K, V> ch = t;
			while (p != null && ch == p.getLeft()) {
				ch = p;
				p = p.getParent();
			}
			return p;
		}
	}

	private static <K extends Comparable<K>, V extends Comparable<V>> void collect(@Nullable final Entry<K, V> e,
			@Nonnull final K start, @Nonnull final K end, @Nonnull final SortableDeque<Tuple<K, V>> results) {
		if (e == null) {
			return;
		}
		if (e.getLeft() != null && e.getLeft().childrenIntersects(start, end)) {
			collect(e.getLeft(), start, end, results);
		}
		if (e.intersects(start, end)) {
			results.pushLast(e);
		}
		if (e.getRight() != null && e.getRight().childrenIntersects(start, end)) {
			collect(e.getRight(), start, end, results);
		}
	}

	@Nullable
	private Entry<K, V> find(@Nonnull final K start, @Nonnull final K end, @Nonnull final V value) {
		if (start == null || end == null || value == null) {
			throw new NullPointerException();
		}
		return find(root, start, end, value);
	}

	@Nullable
	private static <K extends Comparable<K>, V extends Comparable<V>> Entry<K, V> find(
			@Nonnull final Entry<K, V> e, @Nonnull final K start, @Nonnull final K end, @Nonnull final V value) {
		if (e == null) {
			return null;
		}
		if (e.matches(start, end, value)) {
			return e;
		}
		if (e.getLeft() != null && e.getLeft().childrenContains(start, end)) {
			final Entry<K, V> l = find(e.getLeft(), start, end, value);
			if (l != null) {
				return l;
			}
		}
		if (e.getRight() != null && e.getRight().childrenContains(start, end)) {
			final Entry<K, V> r = find(e.getRight(), start, end, value);
			if (r != null) {
				return r;
			}
		}
		return null;
	}

	/**
	 * Balancing operations.
	 * 
	 * Implementations of rebalancings during insertion and deletion are
	 * slightly different than the CLR version. Rather than using dummy
	 * nilnodes, we use a set of accessors that deal properly with null. They
	 * are used to avoid messiness surrounding nullness checks in the main
	 * algorithms.
	 */

	@Nonnull
	private static Color colorOf(@Nullable final Entry<?, ?> p) {
		if (p == null) {
			return BLACK;
		}
		return p.getColor();
	}

	@Nullable
	private static <K extends Comparable<K>, V extends Comparable<V>> Entry<K, V> parentOf(
			@Nullable final Entry<K, V> p) {
		if (p == null) {
			return null;
		}
		return p.getParent();
	}

	private static void setColor(@Nullable final Entry<?, ?> p, final Color c) {
		if (p != null) {
			p.setColor(c);
		}
	}

	@Nullable
	private static <K extends Comparable<K>, V extends Comparable<V>> Entry<K, V> leftOf(
			@Nullable final Entry<K, V> p) {
		if (p == null) {
			return null;
		}
		return p.getLeft();
	}

	@Nullable
	private static <K extends Comparable<K>, V extends Comparable<V>> Entry<K, V> rightOf(
			@Nullable final Entry<K, V> p) {
		if (p == null) {
			return null;
		}
		return p.getRight();
	}

	/** From CLR */
	private void rotateLeft(@Nullable final Entry<K, V> p) {
		if (p == null) {
			return;
		}

		final Entry<K, V> r = p.getRight();
		p.setRight(r.getLeft());
		if (r.getLeft() != null) {
			r.getLeft().setParent(p);
		}
		r.setParent(p.getParent());
		if (p.getParent() == null) {
			root = r;
		} else if (p.getParent().getLeft() == p) {
			p.getParent().setLeft(r);
		} else {
			p.getParent().setRight(r);
		}
		r.setLeft(p);
		p.setParent(r);
		fixMax(p);
		fixMax(r);
	}

	/** From CLR */
	private void rotateRight(@Nullable final Entry<K, V> p) {
		if (p == null) {
			return;
		}

		final Entry<K, V> l = p.getLeft();
		p.setLeft(l.getRight());
		if (l.getRight() != null) {
			l.getRight().setParent(p);
		}
		l.setParent(p.getParent());
		if (p.getParent() == null) {
			root = l;
		} else if (p.getParent().getRight() == p) {
			p.getParent().setRight(l);
		} else {
			p.getParent().setLeft(l);
		}
		l.setRight(p);
		p.setParent(l);
		fixMax(p);
		fixMax(l);
	}

	private static <K extends Comparable<K>, V extends Comparable<V>> void fixMaxRecursive(
			@Nullable final Entry<K, V> e) {
		Entry<K, V> x = e;
		while (x != null) {
			fixMax(x);
			x = x.getParent();
		}
	}

	private static <K extends Comparable<K>, V extends Comparable<V>> void fixMax(@Nullable final Entry<K, V> e) {
		if (e == null) {
			return;
		}
		final boolean noleft = (e.getLeft() == null);
		final boolean noright = (e.getRight() == null);
		// optimized case for no children
		if (noleft && noright) {
			e.setMinStart(e.getStart());
			e.setMaxEnd(e.getEnd());
			return;
		}
		// optimized case for single left child
		if (!noleft && noright) {
			if (e.getStart().compareTo(e.getLeft().getMinStart()) > 0) {
				e.setMinStart(e.getLeft().getMinStart());
			} else {
				e.setMinStart(e.getStart());
			}
			if (e.getEnd().compareTo(e.getLeft().getMaxEnd()) < 0) {
				e.setMaxEnd(e.getLeft().getMaxEnd());
			} else {
				e.setMaxEnd(e.getEnd());
			}
			return;
		}
		// optimized case for single right child
		if (noleft && !noright) {
			if (e.getStart().compareTo(e.getRight().getMinStart()) > 0) {
				e.setMinStart(e.getRight().getMinStart());
			} else {
				e.setMinStart(e.getStart());
			}
			if (e.getEnd().compareTo(e.getRight().getMaxEnd()) < 0) {
				e.setMaxEnd(e.getRight().getMaxEnd());
			} else {
				e.setMaxEnd(e.getEnd());
			}
			return;
		}
		// normal case with two children
		final int cmpStartLeft = e.getStart().compareTo(e.getLeft().getMinStart());
		final int cmpEndLeft = e.getEnd().compareTo(e.getLeft().getMaxEnd());
		final int cmpStartRight = e.getStart().compareTo(e.getRight().getMinStart());
		final int cmpEndRight = e.getEnd().compareTo(e.getRight().getMaxEnd());

		if (cmpStartLeft > 0 && cmpStartRight > 0) {
			if (e.getLeft().getMinStart().compareTo(e.getRight().getMinStart()) <= 0) {
				e.setMinStart(e.getLeft().getMinStart());
			} else {
				e.setMinStart(e.getRight().getMinStart());
			}
		} else if (cmpStartLeft > 0) {
			e.setMinStart(e.getLeft().getMinStart());
		} else if (cmpStartRight > 0) {
			e.setMinStart(e.getRight().getMinStart());
		} else {
			e.setMinStart(e.getStart());
		}
		if (cmpEndLeft < 0 && cmpEndRight < 0) {
			if (e.getLeft().getMaxEnd().compareTo(e.getRight().getMaxEnd()) >= 0) {
				e.setMaxEnd(e.getLeft().getMaxEnd());
			} else {
				e.setMaxEnd(e.getRight().getMaxEnd());
			}
		} else if (cmpEndLeft < 0) {
			e.setMaxEnd(e.getLeft().getMaxEnd());
		} else if (cmpEndRight < 0) {
			e.setMaxEnd(e.getRight().getMaxEnd());
		} else {
			e.setMaxEnd(e.getEnd());
		}
	}

	/** From CLR */
	private void fixAfterInsertion(@Nonnull final Entry<K, V> e) {
		Entry<K, V> x = e;
		x.setColor(RED);

		while (x != null && x != root && x.getParent().getColor() == RED) {
			if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {
				final Entry<K, V> y = rightOf(parentOf(parentOf(x)));
				if (colorOf(y) == RED) {
					setColor(parentOf(x), BLACK);
					setColor(y, BLACK);
					setColor(parentOf(parentOf(x)), RED);
					x = parentOf(parentOf(x));
				} else {
					if (x == rightOf(parentOf(x))) {
						x = parentOf(x);
						rotateLeft(x);
					}
					setColor(parentOf(x), BLACK);
					setColor(parentOf(parentOf(x)), RED);
					rotateRight(parentOf(parentOf(x)));
				}
			} else {
				final Entry<K, V> y = leftOf(parentOf(parentOf(x)));
				if (colorOf(y) == RED) {
					setColor(parentOf(x), BLACK);
					setColor(y, BLACK);
					setColor(parentOf(parentOf(x)), RED);
					x = parentOf(parentOf(x));
				} else {
					if (x == leftOf(parentOf(x))) {
						x = parentOf(x);
						rotateRight(x);
					}
					setColor(parentOf(x), BLACK);
					setColor(parentOf(parentOf(x)), RED);
					rotateLeft(parentOf(parentOf(x)));
				}
			}
		}
		root.setColor(BLACK);
	}

	/** From CLR */
	private void fixAfterDeletion(@Nonnull final Entry<K, V> e) {
		Entry<K, V> x = e;
		while (x != root && colorOf(x) == BLACK) {
			if (x == leftOf(parentOf(x))) {
				Entry<K, V> sib = rightOf(parentOf(x));

				if (colorOf(sib) == RED) {
					setColor(sib, BLACK);
					setColor(parentOf(x), RED);
					rotateLeft(parentOf(x));
					sib = rightOf(parentOf(x));
				}

				if (colorOf(leftOf(sib)) == BLACK && colorOf(rightOf(sib)) == BLACK) {
					setColor(sib, RED);
					x = parentOf(x);
				} else {
					if (colorOf(rightOf(sib)) == BLACK) {
						setColor(leftOf(sib), BLACK);
						setColor(sib, RED);
						rotateRight(sib);
						sib = rightOf(parentOf(x));
					}
					setColor(sib, colorOf(parentOf(x)));
					setColor(parentOf(x), BLACK);
					setColor(rightOf(sib), BLACK);
					rotateLeft(parentOf(x));
					x = root;
				}
			} else { // symmetric
				Entry<K, V> sib = leftOf(parentOf(x));

				if (colorOf(sib) == RED) {
					setColor(sib, BLACK);
					setColor(parentOf(x), RED);
					rotateRight(parentOf(x));
					sib = leftOf(parentOf(x));
				}

				if (colorOf(rightOf(sib)) == BLACK && colorOf(leftOf(sib)) == BLACK) {
					setColor(sib, RED);
					x = parentOf(x);
				} else {
					if (colorOf(leftOf(sib)) == BLACK) {
						setColor(rightOf(sib), BLACK);
						setColor(sib, RED);
						rotateLeft(sib);
						sib = leftOf(parentOf(x));
					}
					setColor(sib, colorOf(parentOf(x)));
					setColor(parentOf(x), BLACK);
					setColor(leftOf(sib), BLACK);
					rotateRight(parentOf(x));
					x = root;
				}
			}
		}

		setColor(x, BLACK);
	}

	@Nonnull
	private IteratorCurrent<V> createIterator(@Nonnull final Entry<K, V> start) {
		return new IteratorCurrent<V>() {
			private Entry<K, V> current = previous(start);
			private Entry<K, V> next = start;

			private Entry<K, V> previous(final Entry<K, V> e) {
				final Entry<K, V> pre = predecessor(e);
				if (pre != null && pre.matches(e.getStart(), e.getEnd())) {
					return pre;
				}
				return null; 
			}

			@Override
			public V current() {
				if (current != null) {
					return current.getValue();
				}
				return null;
			}

			@Override
			public boolean hasNext() {
				return next != null && next.matches(start.getStart(), start.getEnd());
			}

			@Override
			public V next() {
				current = next;
				next = successor(next);
				if (current != null) {
					return current.getValue();
				}
				throw new NoSuchElementException();
			}

			@Override
			public void remove() {
				final Entry<K, V> e = current;
				current = previous(current);
				IntervalTreeMap.this.remove(e.getStart(), e.getEnd(), e.getValue());
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
	static <K extends Comparable<K>, V extends Comparable<V>> Tuple<K, V> asTuple(
			@Nonnull final K start, @Nonnull final K end, @Nonnull final V value) {
		return new Entry<>(start, end, value, null);
	}

	/**
	 * @author Ricardo Padilha
	 */
	private enum Color {
		RED,
		BLACK;
	}

	/**
	 * @author Ricardo Padilha
	 */
	interface Tuple<K, V> extends Comparable<Tuple<K, V>> {

		@Nonnull
		K getStart();

		@Nonnull
		K getEnd();

		@Nonnull
		V getValue();

		boolean matches(@Nonnull K start, @Nonnull K end, @Nonnull V value);
	}

	/**
	 * Node in the Tree.
	 */
	private static final class Entry<K extends Comparable<K>, V extends Comparable<V>> implements Tuple<K, V> {

		@Nonnull
		private K start;
		@Nonnull
		private K end;
		@Nonnull
		private K minStart;
		@Nonnull
		private K maxEnd;
		@Nonnull
		private V value;

		@Nullable
		private Entry<K, V> left;
		@Nullable
		private Entry<K, V> right;
		@Nullable
		private Entry<K, V> parent;
		@Nonnull
		private Color color;

		/**
		 * Make a new cell with given key, value, and parent, and with
		 * {@code null} child links, and BLACK color.
		 */
		Entry(@Nonnull final K start, @Nonnull final K end, @Nonnull final V value,
				@Nullable final Entry<K, V> parent) {
			if (start == null || end == null || value == null) {
				throw new NullPointerException();
			}
			this.start = start;
			this.end = end;
			this.value = value;
			this.minStart = start;
			this.maxEnd = end;
			this.left = null;
			this.right = null;
			this.parent = parent;
			this.color = Color.BLACK;
		}

		void setParent(@Nullable final Entry<K, V> parent) {
			this.parent = parent;
		}

		@Nullable
		Entry<K, V> getParent() {
			return parent;
		}

		void setLeft(@Nullable final Entry<K, V> left) {
			this.left = left;
		}

		@Nullable
		Entry<K, V> getLeft() {
			return left;
		}

		void setRight(@Nullable final Entry<K, V> right) {
			this.right = right;
		}

		@Nullable
		Entry<K, V> getRight() {
			return right;
		}

		void setColor(@Nonnull final Color color) {
			this.color = color;
		}

		@Nonnull
		Color getColor() {
			return color;
		}

		void setStart(@Nonnull final K start) {
			this.start = start;
		}

		@Override
		public K getStart() {
			return start;
		}

		void setEnd(@Nonnull final K end) {
			this.end = end;
		}

		@Override
		public K getEnd() {
			return end;
		}

		void setMaxEnd(@Nonnull final K maxEnd) {
			this.maxEnd = maxEnd;
		}

		@Nonnull
		K getMaxEnd() {
			return maxEnd;
		}

		void setMinStart(@Nonnull final K minStart) {
			this.minStart = minStart;
		}

		@Nonnull
		K getMinStart() {
			return minStart;
		}

		void setValue(@Nonnull final V value) {
			this.value = value;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public int compareTo(final Tuple<K, V> o) {
			if (o == null) {
				return 1;
			}
			if (o == this) {
				return 0;
			}
			return value.compareTo(o.getValue());
		}

		@Override
		public boolean equals(final Object obj) {
			if (obj == this) {
				return true;
			}
			if (obj instanceof Tuple) {
				final Tuple<?, ?> t = (Tuple<?, ?>) obj;
				return start.equals(t.getStart()) && end.equals(t.getEnd()) && value.equals(t.getValue());
			}
			return false;
		}

		@Override
		public int hashCode() {
			return Objects.hash(start, end, value);
		}

		boolean matches(@Nonnull final K start, @Nonnull final K end) {
			return this.start.equals(start) && this.end.equals(end);
		}

		@Override
		public boolean matches(final K start, final K end, final V value) {
			return this.start.equals(start) && this.end.equals(end) && this.value.equals(value);
		}

		boolean intersects(@Nonnull final K start, @Nonnull final K end) {
			return this.start.compareTo(end) <= 0 && this.end.compareTo(start) >= 0;
		}

		boolean childrenIntersects(@Nonnull final K start, @Nonnull final K end) {
			return this.minStart.compareTo(end) <= 0 && this.maxEnd.compareTo(start) >= 0;
		}

		boolean childrenContains(@Nonnull final K start, @Nonnull final K end) {
			return this.minStart.compareTo(start) <= 0 && this.maxEnd.compareTo(end) >= 0;
		}
	}
}
