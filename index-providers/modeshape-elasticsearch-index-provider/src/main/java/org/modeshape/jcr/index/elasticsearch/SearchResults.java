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

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.modeshape.jcr.cache.NodeKey;
import org.modeshape.jcr.spi.index.Index;
import org.modeshape.jcr.spi.index.ResultWriter;

/**
 *
 * @author kulikov
 */
public class SearchResults implements Index.Results {

    private final SearchRequestBuilder searchBuilder;
    private SearchResponse response;
    private int pos = 0;

    /**
     * Creates new search result instance for the given search request.
     * 
     * @param searchBuilder search request as request builder.
     */
    public SearchResults(SearchRequestBuilder searchBuilder) {
        this.searchBuilder = searchBuilder;
        this.response = null;
    }

    @Override
    public boolean getNextBatch(ResultWriter writer, int batchSize) {
        try {
            response = searchBuilder
                    .setSize(batchSize)
                    .setFrom(pos)
                    .execute()
                    .actionGet();
        } catch (IndexMissingException e) {
            return false;
        }

        SearchHits hits = response.getHits();
        SearchHit[] res = hits.hits();

        int amount = Math.min(batchSize, res.length);

        for (int i = 0; i < amount; i++) {
            SearchHit hit = hits.getAt(i);
            writer.add(new NodeKey(hit.getId()), hit.getScore());
        }

        pos += amount;
        return pos < hits.getTotalHits();
    }

    @Override
    public void close() {
    }

    /**
     * Gets cardinality for this search request.
     * 
     * @return total hits matching search request.
     */
    public long getCardinality() {
        try {
            response = searchBuilder
                    .execute()
                    .actionGet();
            return response.getHits().getTotalHits();
        } catch (IndexMissingException e) {
            return 0;
        }
    }
}
