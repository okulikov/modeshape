/*
 * ModeShape (http://www.modeshape.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.modeshape.jcr.index.elasticsearch;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Set of index columns.
 * 
 * @author kulikov
 */
public class EsIndexColumns {
    
    private Map<String, EsIndexColumn> columns = new HashMap();
    
    /**
     * Creates new set.
     * 
     * @param cols 
     */
    public EsIndexColumns(EsIndexColumn... cols) {
        for (int i = 0; i < cols.length; i++) {
            columns.put(cols[i].getName(), cols[i]);
        }
    }
    
    /**
     * Gets column with given name.
     * 
     * @param name
     * @return 
     */
    public EsIndexColumn column(String name) {
        return columns.get(noprefix(name, 
                EsIndexColumn.LENGTH_PREFIX,
                EsIndexColumn.LOWERCASE_PREFIX,
                EsIndexColumn.UPPERCASE_PREFIX));
    }
    
    private String noprefix(String name, String... prefix) {
        for (int i = 0; i < prefix.length; i++) {
            if (name.startsWith(prefix[i])) {
                name = name.replaceAll(prefix[i], "");
            }
        }
        return name;
    }
    
    public Collection<EsIndexColumn> columns() {
        return columns.values();
    }
}
