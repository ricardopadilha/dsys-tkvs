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

import static com.sleepycat.je.LockMode.READ_UNCOMMITTED;
import static com.sleepycat.je.OperationStatus.NOTFOUND;
import static com.sleepycat.je.OperationStatus.SUCCESS;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.api.exception.Bug;
import net.dsys.tkvs.api.data.Key;
import net.dsys.tkvs.api.data.Value;
import net.dsys.tkvs.api.storage.InitializableStorage;
import net.dsys.tkvs.api.storage.TransactionalStorage;
import net.dsys.tkvs.api.transaction.TID;
import net.dsys.tkvs.impl.data.Keys;
import net.dsys.tkvs.impl.data.Values;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * @author Ricardo Padilha
 */
final class BerkleyDBTxStorage implements TransactionalStorage, InitializableStorage {

	@Nonnull
	private final Map<TID, Transaction> pending;
	@Nonnull
	private final Environment env;
	@Nonnull
	private final Database db;
	@Nonnull
	private final Database ccdb;
	@Nonnull
	private final EntryBinding<Key> keyBinding;
	@Nonnull
	private final EntryBinding<Value> valueBinding;

	private Transaction currentTx;

	private BerkleyDBTxStorage(@Nonnull final EnvironmentConfig envConfig,
			@Nonnull final DatabaseConfig dbConfig,
			@Nonnull final File file,
			@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		if (envConfig == null) {
			throw new NullPointerException("envConfig == null");
		}
		if (dbConfig == null) {
			throw new NullPointerException("dbConfig == null");
		}
		if (file == null) {
			throw new NullPointerException("file == null");
		}
		if (dbName == null) {
			throw new NullPointerException("dbName == null");
		}

		this.pending = new HashMap<>(pendingCapacity);
		this.env = new Environment(file, envConfig);
		this.db = env.openDatabase(null, dbName, dbConfig);
		this.ccdb = env.openDatabase(null, "ClassCatalogDB", dbConfig);
		final ClassCatalog classCatalog = new StoredClassCatalog(ccdb);
		this.keyBinding = new SerialBinding<>(classCatalog, Key.class);
		this.valueBinding = new SerialBinding<>(classCatalog, Value.class);
	}

	static BerkleyDBTxStorage inTempFile(@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		final EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setDurability(Durability.COMMIT_NO_SYNC);

		final DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		dbConfig.setTransactional(true);
		dbConfig.setDeferredWrite(true);

		final File file;
		try {
			file = File.createTempFile("TxStorage", "bdb");
		} catch (final IOException e) {
			throw new IllegalStateException("unable to create a temporary file", e);
		}
		return new BerkleyDBTxStorage(envConfig, dbConfig, file, dbName, pendingCapacity);
	}

	static BerkleyDBTxStorage inFile(@Nonnull final Path path,
			@Nonnull final String dbName,
			@Nonnegative final int pendingCapacity) {
		final EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setDurability(Durability.COMMIT_SYNC);

		final DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		dbConfig.setTransactional(true);
		dbConfig.setDeferredWrite(false);

		return new BerkleyDBTxStorage(envConfig, dbConfig, path.toFile(), dbName, pendingCapacity);
	}

	@Nonnull
	private DatabaseEntry convertKey(@Nonnull final Key key) {
		final DatabaseEntry keyHolder = new DatabaseEntry();
		keyBinding.objectToEntry(key, keyHolder);
		return keyHolder;
	}

	@Nonnull
	private DatabaseEntry convertValue(@Nonnull final Value value) {
		final DatabaseEntry valueHolder = new DatabaseEntry();
		valueBinding.objectToEntry(value, valueHolder);
		return valueHolder;
	}

	@Nonnull
	private Key deconvertKey(@Nonnull final DatabaseEntry keyHolder) {
		return keyBinding.entryToObject(keyHolder);
	}

	@Nonnull
	private Value deconvertValue(@Nonnull final DatabaseEntry valueHolder) {
		return valueBinding.entryToObject(valueHolder);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void load(final Map<Key, Value> initialContent) {
		if (initialContent == null) {
			throw new NullPointerException("initialContent == null");
		}
		final Transaction tx = env.beginTransaction(null, null);
		for (final Entry<Key, Value> e : initialContent.entrySet()) {
			db.put(tx, convertKey(e.getKey()), convertValue(e.getValue()));
		}
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
		currentTx = env.beginTransaction(null, null);
		pending.put(tid, currentTx);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean exists(final Key key) {
		if (Keys.isAny(key)) {
			return db.count() != 0;
		}
		final Value value = read(key);
		return !Values.isNull(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Value read(final Key key) {
		final DatabaseEntry keyHolder = convertKey(key);
		final DatabaseEntry valueHolder = new DatabaseEntry();
		final OperationStatus status = db.get(currentTx, keyHolder, valueHolder, READ_UNCOMMITTED);
		if (status == SUCCESS) {
			return deconvertValue(valueHolder);
		}
		if (status == NOTFOUND) {
			return Values.nullValue();
		}
		throw new IllegalStateException(String.valueOf(status));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key next(final Key key, final boolean includeKey) {
		final DatabaseEntry keyHolder = new DatabaseEntry();
		final DatabaseEntry valueHolder = new DatabaseEntry();

		final CursorConfig curConfig = new CursorConfig();
		curConfig.setReadUncommitted(true);

		try (final Cursor cursor = db.openCursor(currentTx, curConfig)) {
			OperationStatus status;
			status = cursor.getSearchKeyRange(keyHolder, valueHolder, READ_UNCOMMITTED);
			if (status == SUCCESS) {
				final Key first = deconvertKey(keyHolder);
				if (includeKey || !key.equals(first)) {
					return first;
				}
				// !includeKey && key.equals(first)
				status = cursor.getNext(keyHolder, valueHolder, READ_UNCOMMITTED);
				if (status == SUCCESS) {
					return deconvertKey(keyHolder);
				}
			}
			if (status == NOTFOUND) {
				return Keys.nullKey();
			}
			throw new IllegalStateException(String.valueOf(status));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key previous(final Key key, final boolean includeKey) {
		final DatabaseEntry keyHolder = new DatabaseEntry();
		final DatabaseEntry valueHolder = new DatabaseEntry();

		final CursorConfig curConfig = new CursorConfig();
		curConfig.setReadUncommitted(true);

		try (final Cursor cursor = db.openCursor(currentTx, curConfig)) {
			OperationStatus status;
			status = cursor.getSearchKeyRange(keyHolder, valueHolder, READ_UNCOMMITTED);
			if (status == SUCCESS) {
				final Key first = deconvertKey(keyHolder);
				if (includeKey || !key.equals(first)) {
					return first;
				}
				// !includeKey && key.equals(first)
				status = cursor.getPrev(keyHolder, valueHolder, READ_UNCOMMITTED);
				if (status == SUCCESS) {
					return deconvertKey(keyHolder);
				}
			}
			if (status == NOTFOUND) {
				return Keys.nullKey();
			}
			throw new IllegalStateException(String.valueOf(status));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Key key, final Value value) {
		final DatabaseEntry keyHolder = convertKey(key);
		final DatabaseEntry valueHolder = convertValue(value);
		final OperationStatus status = db.put(currentTx, keyHolder, valueHolder);
		if (status != SUCCESS) {
			throw new IllegalStateException(String.valueOf(status));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete(final Key key) {
		final DatabaseEntry keyHolder = convertKey(key);
		final OperationStatus status = db.delete(currentTx, keyHolder);
		if (status != SUCCESS) {
			throw new IllegalStateException(String.valueOf(status));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		final DatabaseEntry keyHolder = new DatabaseEntry();
		final DatabaseEntry valueHolder = new DatabaseEntry();

		final CursorConfig curConfig = new CursorConfig();
		curConfig.setReadUncommitted(true);

		try (final Cursor cursor = db.openCursor(currentTx, curConfig)) {
			OperationStatus status;
			status = cursor.getFirst(keyHolder, valueHolder, READ_UNCOMMITTED);
			while (status == SUCCESS) {
				db.delete(currentTx, keyHolder);
				status = cursor.getNext(keyHolder, valueHolder, READ_UNCOMMITTED);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void end() {
		currentTx = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void finish(final TID tid, final boolean commit) {
		if (tid == null) {
			throw new NullPointerException("tid == null");
		}
		final Transaction tx = pending.remove(tid);
		if (tx == null) {
			throw new IllegalStateException("missing state for " + tid);
		}
		if (commit) {
			tx.commit();
		} else {
			tx.abort();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reset() {
		for (final Transaction tx : pending.values()) {
			tx.abort();
		}
		pending.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		reset();
		db.close();
		ccdb.close();
		env.close();
	}

}
