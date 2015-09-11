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

import java.io.InputStream;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import org.junit.Test;
import org.modeshape.common.util.FileUtil;
import org.modeshape.jcr.EsValidateQuery;
import org.modeshape.jcr.LocalIndexProviderTest;
import org.modeshape.jcr.ValidateQuery;
import org.modeshape.jcr.api.JcrTools;
import org.modeshape.jcr.api.query.Query;

/**
 * Unit test for {@link LuceneIndexProvider}. Since this is a local provider in term of repository locality, we want to run
 * at least the same tests as for {@link org.modeshape.jcr.index.local.LocalIndexProvider}
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class EsIndexProviderTest extends LocalIndexProviderTest {

    @Override
    public void beforeEach() throws Exception {
        FileUtil.delete("target/persistent_repository");
        FileUtil.delete("data");
        startRepositoryWithConfiguration(repositoryConfiguration());
        printMessage("Started repository...");
        tools = new JcrTools();
    }

    @Override
    public void afterEach() throws Exception {
        stopRepository();
        Thread.currentThread().sleep(500);
        FileUtil.delete("target/persistent_repository");
        FileUtil.delete("data");
    }
    
    
    @Override
    protected boolean startRepositoryAutomatically() {
        return false;
    }

    @Override
    protected InputStream repositoryConfiguration() {
        return resource("config/repo-config1.json");
    }

    @Override
    protected String providerName() {
        return "elasticsearch";
    }
    
    @Override
    protected ValidateQuery.ValidationBuilder validateQuery() { 
        return EsValidateQuery.validateQuery().printDetail(print);
    }
    
    @Test
    @Override
    public void shouldSelectIndexWhenMultipleAndedConstraintsApply() throws Exception {
        registerValueIndex("longValues", "nt:unstructured", "Long values index", "*", "value", PropertyType.LONG);

        Node root = session().getRootNode();
        int valuesCount = 5;
        for (int i = 0; i < valuesCount; i++) {
            String name = String.valueOf(i+1);
            Node node = root.addNode(name);
            node.setProperty("value", (long) (i+1));
        }
        session.save();

        String sql1 = "SELECT number.[jcr:name] FROM [nt:unstructured] as number WHERE (number.value > 1 AND number.value < 3) OR " +
                      "(number.value > 3 AND number.value < 5)";
        String sql2 = "SELECT number.[jcr:name] FROM [nt:unstructured] as number WHERE number.value <2";
        Query query = jcrSql2Query(sql1 + " UNION " + sql2);         
        validateQuery()
                .rowCount(3L)
                .useIndex("longValues")
                .hasNodesAtPaths("/2", "/4", "/1")
                .validate(query, query.execute());
    }
    
    @Test
    @Override
    public void shouldUseIndexesAfterRestarting() throws Exception {
        registerValueIndex("pathIndex", "nt:unstructured", "Node path index", "*", "someProperty", PropertyType.STRING);

        // print = true;

        Node root = session().getRootNode();
        // Add a node that uses this type ...
        Node book1 = root.addNode("myFirstBook");
        book1.addMixin("mix:title");
        book1.setProperty("jcr:title", "The Title");
        book1.setProperty("someProperty", "value1");

        Node book2 = root.addNode("mySecondBook");
        book2.addMixin("mix:title");
        book2.setProperty("jcr:title", "A Different Title");
        book2.setProperty("someProperty", "value2");

        Node other = book2.addNode("chapter");
        other.setProperty("propA", "a value for property A");
        other.setProperty("jcr:title", "The Title");
        other.setProperty("someProperty", "value1");

        session.save();

        // Issues some queries that should use this index ...
        final String queryStr = "SELECT * FROM [nt:unstructured] WHERE someProperty = 'value1'";
        Query query = jcrSql2Query(queryStr);
        validateQuery().rowCount(2L).useIndex("pathIndex").validate(query, query.execute());

        // Shutdown the repository and restart it ...
        stopRepository();
        printMessage("Stopped repository. Restarting ...");
        startRepositoryWithConfiguration(repositoryConfiguration());
        printMessage("Repository restart complete");

        // Issues the same query and verify it uses an index...
        query = jcrSql2Query(queryStr);
        validateQuery().rowCount(2L).useIndex("pathIndex").validate(query, query.execute());
    }
    
    @Test
    @Override
    public void shouldUseSingleColumnResidualPropertyIndexInQueryAgainstSameNodeType() throws Exception {
    }    
}
