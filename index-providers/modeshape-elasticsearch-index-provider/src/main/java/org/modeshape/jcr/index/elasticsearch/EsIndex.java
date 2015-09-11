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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jcr.query.qom.Constraint;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.modeshape.common.util.CheckArg;
import org.modeshape.jcr.ExecutionContext;
import org.modeshape.jcr.api.index.IndexColumnDefinition;
import org.modeshape.jcr.api.index.IndexDefinition;
import org.modeshape.jcr.spi.index.IndexConstraints;
import org.modeshape.jcr.spi.index.provider.ProvidedIndex;

/**
 * Elasticsearch based index.
 *
 * @author kulikov
 */
public class EsIndex implements ProvidedIndex {

    private final String name;
    private final String workspace;
    private final Client client;
    private final Node node;
    private final ExecutionContext context;
    private final EsIndexColumns columns;
    private final Operations operations;
    
    public EsIndex(ExecutionContext context, IndexDefinition defn, String workspace, Node node, Client client) {
        this.context = context;
        this.name = defn.getName();
        this.workspace = workspace;
        this.node = node;
        this.client = client;
        this.columns = defineEsIndexColumns(defn);
        this.operations = new Operations(context.getValueFactories(), columns);
        this.createIndex();
    }

    protected EsIndex(EsIndexColumns columns, ExecutionContext context, String name, String workspace, Node node, Client client) {
        this.context = context;
        this.name = name;
        this.workspace = workspace;
        this.node = node;
        this.client = client;
        this.columns = columns;
        this.operations = new Operations(context.getValueFactories(), columns);
        this.createIndex();
    }

    private EsIndexColumns defineEsIndexColumns(IndexDefinition defn) {
        EsIndexColumn[] cols = new EsIndexColumn[defn.size()];
        for (int i = 0; i < cols.length; i++) {
            IndexColumnDefinition idef = defn.getColumnDefinition(i);
            cols[i] = new EsIndexColumn(context, idef.getPropertyName(), idef.getColumnType());
        }
        return new EsIndexColumns(cols);
    }
    
    private XContentBuilder defineMappingForIndexType() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject()
                .startObject(workspace).startObject("properties");
        for (EsIndexColumn column : columns.columns()) {
            column.append(builder);
        }
        return builder.endObject().endObject().endObject();
    }
    
    private String name() {
        return name.toLowerCase();
    }

    
    private void createIndex() {
        IndicesExistsResponse existsResponse = 
                client.admin().indices().prepareExists(name()).execute().actionGet();
        if (existsResponse.isExists()) {
            client.admin().indices().prepareDelete(name()).execute().actionGet();
        }
        
        try {
            XContentBuilder mapping = defineMappingForIndexType();
            client.admin().indices().prepareCreate(name())
                    .addMapping(workspace, mapping).execute().actionGet();
            client.admin().indices().prepareRefresh(name()).execute().actionGet();
        } catch (IndexAlreadyExistsException | IOException e) {
        }
    }
    
    @Override
    public void add(String nodeKey, String propertyName, Object value) {
        CheckArg.isNotNull(nodeKey, "nodeKey");
        CheckArg.isNotNull(propertyName, "propertyName");
        CheckArg.isNotNull(value, "value");

        EsIndexColumn column = columns.column(propertyName);
        assert column != null : "Unexpected column for the index " + name();
        
        Map<String, Object> doc = findOrCreateDoc(nodeKey);
        putValue(doc, column, value);
        
        client.prepareIndex(name(), workspace, nodeKey).setSource(doc).execute().actionGet();
        client.admin().indices().prepareRefresh(name()).execute().actionGet();
    }

    @Override
    public void add(String nodeKey, String propertyName, Object[] values) {
        CheckArg.isNotNull(nodeKey, "nodeKey");
        CheckArg.isNotNull(propertyName, "propertyName");
        CheckArg.isNotNull(values, "values");

        EsIndexColumn column = columns.column(propertyName);
        assert column != null : "Unexpected column for the index " + name();
        
        Map<String, Object> doc = findOrCreateDoc(nodeKey);
        putValues(doc, column, values);
        client.prepareIndex(name(), workspace, nodeKey).setSource(doc).execute().actionGet();
        client.admin().indices().prepareRefresh(name()).execute().actionGet();
    }

    @Override
    public void remove(String nodeKey) {
        CheckArg.isNotNull(nodeKey, "nodeKey");
        client.prepareDelete(name(), workspace, nodeKey).execute().actionGet();
        client.admin().indices().prepareRefresh(name()).execute().actionGet();
    }

    @Override
    public void remove(String nodeKey, String propertyName, Object value) {
        CheckArg.isNotNull(nodeKey, "nodeKey");
        CheckArg.isNotNull(propertyName, "propertyName");
        
        Map<String, Object> doc = find(nodeKey);

        if (doc == null) {
            return;
        }

        doc.remove(propertyName);
        
        client.prepareIndex(name(), workspace, nodeKey).setSource(doc).execute().actionGet();
        client.admin().indices().prepareRefresh(name()).execute().actionGet();
    }

    @Override
    public void remove(String nodeKey, String propertyName, Object[] values) {
        CheckArg.isNotNull(nodeKey, "nodeKey");
        CheckArg.isNotNull(propertyName, "propertyName");

        Map<String, Object> doc = find(nodeKey);
        if (doc == null) {
            return;
        }

        doc.remove(propertyName);

        try {
            client.prepareIndex(name(), workspace, nodeKey).setSource(doc).execute().actionGet();
            client.admin().indices().prepareRefresh(name()).execute().actionGet();
        } catch (IndexMissingException e) {
        }
    }

    private Map<String, Object> find(String nodeKey) {
        try {
            GetResponse resp = client.prepareGet(name(), workspace, nodeKey)
                    .execute().actionGet();
            return resp != null ? resp.getSource() : null;
        } catch (Exception e) {
            return null;
        }
    }

    private Map<String, Object> findOrCreateDoc(String nodeKey) {
        GetResponse resp = null;

        try {
            resp = client.prepareGet(name(), workspace, nodeKey)
                    .execute().actionGet();
        } catch (Exception e) {
            return new HashMap();
        }

        if (resp == null || resp.getSource() == null) {
            return new HashMap();
        }

        return resp.getSource();
    }

    private void putValue(Map doc, EsIndexColumn column, Object value) {
        Object columnValue = column.columnValue(value);
        String stringValue = column.stringValue(value);
        doc.put(column.getName(), columnValue);
        doc.put(column.getLowerCaseFieldName(), stringValue.toLowerCase());
        doc.put(column.getUpperCaseFieldName(), stringValue.toUpperCase());
        doc.put(column.getLengthFieldName(), stringValue.length());
    }

    private void putValues(Map doc, EsIndexColumn column, Object[] value) {
        Object[] columnValue = column.columnValues(value);
        int[] ln = new int[columnValue.length];
        String[] lc = new String[columnValue.length];
        String[] uc = new String[columnValue.length];
        
        for (int i = 0; i < columnValue.length; i++) {            
            String stringValue = column.stringValue(columnValue[i]);
            lc[i] = stringValue.toLowerCase();
            uc[i] = stringValue.toUpperCase();
            ln[i] = stringValue.length();
        }
        
        doc.put(column.getName(), columnValue);
        doc.put(column.getLowerCaseFieldName(), lc);
        doc.put(column.getUpperCaseFieldName(), uc);
        doc.put(column.getLengthFieldName(), ln);        
    }
    
    @Override
    public void commit() {
        client.admin().indices().prepareFlush(name()).execute().actionGet();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Results filter(IndexConstraints constraints) {
        FilterBuilder filter = operations.createFilter(constraints.getConstraints(), constraints.getVariables());
        try {
            SearchRequestBuilder searchBuilder = client
                    .prepareSearch(name())
                    .setTypes(workspace)
                    .setPostFilter(filter);
            return new SearchResults(searchBuilder);
        } catch (IndexMissingException e) {
            return new EmptyResults();
        }
    }

    @Override
    public long estimateCardinality(List<Constraint> constraints, Map<String, Object> variables) {
        try {
            FilterBuilder filter = operations.createFilter(constraints, variables);
            SearchRequestBuilder searchBuilder = client
                    .prepareSearch(name())
                    .setTypes(workspace)
                    .setPostFilter(filter);
            return new SearchResults(searchBuilder).getCardinality();
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public long estimateTotalCount() {
        try {
            CountResponse res = client.prepareCount(name())
                    .setQuery(QueryBuilders.termQuery("_type", workspace))
                    .execute().actionGet();
            return res.getCount();
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public boolean requiresReindexing() {
        return !client.admin()
                .indices().prepareExists(name(), workspace).execute()
                .actionGet().isExists();
    }

    @Override
    public void clearAllData() {
        try {
            client.admin().indices().prepareDelete(name()).execute().actionGet();
        } catch (Exception e) {
        }
    }

    @Override
    public void shutdown(boolean destroyed) {
        try {
            if (destroyed) {
                try {
                    client.admin().indices().prepareDelete(name(), workspace)
                            .execute().actionGet();
                } catch (Exception e) {
                }
            }            
        } finally {
            node.close();
        }
    }
    
}
