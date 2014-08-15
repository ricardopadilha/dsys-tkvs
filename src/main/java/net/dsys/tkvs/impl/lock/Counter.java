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

import java.util.concurrent.atomic.AtomicInteger;

import net.dsys.commons.api.exception.Bug;
import net.dsys.tkvs.api.lock.Lock;

/**
 * @author Ricardo Padilha
 */
final class Counter implements Lock {

	private final AtomicInteger counter;

	Counter() {
		this.counter = new AtomicInteger();
	}

	@Override
	public void acquire() {
		counter.incrementAndGet();
	}

	@Override
	public void acquire(final int n) {
		if (n < 0) {
			throw new IllegalArgumentException("n < 0");
		}
		counter.addAndGet(n);
	}

	@Override
	public void release() {
		if (counter.get() == 0) {
			throw new Bug("decrementing a zero-counter");
		}
		counter.decrementAndGet();
	}

	@Override
	public boolean isFree() {
		return counter.get() == 0;
	}

	@Override
	public String toString() {
		return counter.toString();
	}
}
