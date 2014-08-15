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

package net.dsys.tkvs.impl.storage;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.api.exception.Bug;
import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.data.Value;
import net.dsys.tkvs.api.storage.InitializableStorage;
import net.dsys.tkvs.api.storage.TransactionalStorage;
import net.dsys.tkvs.api.transaction.TID;
import net.dsys.tkvs.impl.data.Keys;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;

/**
 * @author Ricardo Padilha
 */
final class MapDBTxStorage implements TransactionalStorage, InitializableStorage {

	@Nonnull
	private final TxMaker txMaker;
	@Nonnull
	private final String dbName;
	@Nonnull
	private final Map<TID, DB> pending;

	private NavigableMap<Key, Value> currentMap;

	private MapDBTxStorage(@Nonnull final TxMaker txMaker,
			@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		if (txMaker == null) {
			throw new NullPointerException("txMaker == null");
		}
		if (dbName == null) {
			throw new NullPointerException("dbName == null");
		}
		this.txMaker = txMaker;
		this.dbName = dbName;
		this.pending = new HashMap<>(pendingCapacity);
	}

	static MapDBTxStorage inMemory(@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		final TxMaker txMaker = DBMaker.newMemoryDB().makeTxMaker();
		return new MapDBTxStorage(txMaker, dbName, pendingCapacity);
	}

	static MapDBTxStorage inTempFile(@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		final TxMaker txMaker = DBMaker.newTempFileDB().makeTxMaker();
		return new MapDBTxStorage(txMaker, dbName, pendingCapacity);
	}

	static MapDBTxStorage inFile(@Nonnull final Path path,
			@Nonnull final String dbName, @Nonnegative final int pendingCapacity) {
		final TxMaker txMaker = DBMaker.newFileDB(path.toFile()).makeTxMaker();
		return new MapDBTxStorage(txMaker, dbName, pendingCapacity);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void load(final Map<Key, Value> initialContent) {
		if (initialContent == null) {
			throw new NullPointerException("initialContent == null");
		}
		final DB tx = txMaker.makeTx();
		final Map<Key, Value> map = tx.getTreeMap(dbName);
		map.putAll(initialContent);
		tx.commit();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void start(final TID tid) {
		if (pending.containsKey(tid)) {
			throw new Bug("TID already started: " + tid);
		}
		final DB db = txMaker.makeTx();
		pending.put(tid, db);
		currentMap = db.getTreeMap(dbName);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean exists(final Key key) {
		if (Keys.isAny(key)) {
			return !currentMap.isEmpty();
		}
		return currentMap.containsKey(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Value read(final Key key) {
		return currentMap.get(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key next(final Key key, final boolean includeKey) {
		if (includeKey) {
			return currentMap.ceilingKey(key);
		}
		return currentMap.higherKey(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key previous(final Key key, final boolean includeKey) {
		if (includeKey) {
			return currentMap.floorKey(key);
		}
		return currentMap.lowerKey(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Key key, final Value value) {
		currentMap.put(key, value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete(final Key key) {
		currentMap.remove(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		currentMap.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void end() {
		currentMap = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void finish(final TID tid, final boolean commit) {
		if (tid == null) {
			throw new NullPointerException("tid == null");
		}
		final DB db = pending.remove(tid);
		if (db == null) {
			throw new IllegalStateException("missing state for " + tid);
		}
		if (commit) {
			db.commit();
		} else {
			db.rollback();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reset() {
		for (final DB db : pending.values()) {
			db.rollback();
		}
		pending.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		reset();
		txMaker.close();
	}

}
