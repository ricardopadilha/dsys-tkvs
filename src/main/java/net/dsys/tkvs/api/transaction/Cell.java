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

import javax.annotation.Nonnull;

/**
 * @author Ricardo Padilha
 */
public interface Cell {

	/**
	 * @param tx
	 *            access to the transaction's state
	 * @return outcome of the execution for this method. Implementors can only
	 *         return {@link Status#}
	 */
	void execute(@Nonnull Transaction tx);

}
