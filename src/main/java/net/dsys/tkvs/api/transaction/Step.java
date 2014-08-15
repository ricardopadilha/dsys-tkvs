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

package net.dsys.tkvs.api.transaction;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Interface for the code that is to be executed in a given round, for a given
 * partition.
 * 
 * @author Ricardo Padilha
 */
public interface Step {

	/**
	 * @param table
	 *            access to the key-value store table.
	 * @param params
	 *            contains the transaction's parameters.
	 * @param state
	 *            contains the transaction's shared state, i.e., data that has
	 *            been exported.
	 * @param results
	 *            contains the transaction's results, which will be returned to
	 *            the client at commit.
	 * @return outcome of the execution for this method. Implementors can only return {@link Status#}
	 */
	@Nonnull
	Status execute(@Nonnull Database db,
			@Nonnull Map<String, Object> params,
			@Nonnull Map<String, Object> state,
			@Nonnull List<Object> results);

}
