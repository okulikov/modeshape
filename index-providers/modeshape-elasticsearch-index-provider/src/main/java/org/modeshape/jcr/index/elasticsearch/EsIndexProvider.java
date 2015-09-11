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
import javax.jcr.RepositoryException;
import javax.jcr.query.qom.ChildNodeJoinCondition;
import javax.jcr.query.qom.DescendantNodeJoinCondition;
import javax.jcr.query.qom.DynamicOperand;
import javax.jcr.query.qom.JoinCondition;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.modeshape.common.collection.Problems;
import org.modeshape.jcr.ExecutionContext;
import org.modeshape.jcr.NodeTypes;
import org.modeshape.jcr.api.index.IndexDefinition;
import org.modeshape.jcr.api.query.qom.ChildCount;
import org.modeshape.jcr.cache.change.ChangeSetAdapter;
import org.modeshape.jcr.query.QueryContext;
import org.modeshape.jcr.query.model.FullTextSearch;
import org.modeshape.jcr.query.model.Or;
import org.modeshape.jcr.spi.index.IndexCostCalculator;
import org.modeshape.jcr.spi.index.provider.IndexProvider;
import org.modeshape.jcr.spi.index.provider.IndexUsage;
import org.modeshape.jcr.spi.index.provider.ManagedIndexBuilder;

/**
 *
 * @author kulikov
 */
public class EsIndexProvider extends IndexProvider {

    private final static int NUMBER_OF_TRIES = 5;
    private final static String LOCAL = "local";
    private String clusterName = LOCAL;
    private Node esNode;
    private Client client;

    @Override
    protected void doInitialize() throws RepositoryException {
        //wait for green light
        int count = 0;
        ClusterHealthResponse response;

        do {
            disconnect();
            connect();

            logger().debug("Elasticsearch index provider for repository '{1}' "
                    + "is trying to connect to cluster '{2}'", getRepositoryName(), clusterName);
            response = client.admin().cluster()
                    .prepareHealth().setWaitForGreenStatus().execute().actionGet();
            count++;
        } while (response.getStatus() != ClusterHealthStatus.GREEN && count < NUMBER_OF_TRIES);

        if (response.getStatus() != ClusterHealthStatus.GREEN) {
            this.postShutdown();
            logger().error("Could not connect to cluster " + response.getStatus());
            throw new RepositoryException("Could not connect to elasticsearch cluster: " + clusterName);
        }

        logger().debug("Elasticsearch index provider for repository '{1}' "
                + "got green light for cluster '{2}'", getRepositoryName(), clusterName);
    }

    private void disconnect() {
        try {
            //close client
            if (client != null) {
                client.close();
            }

            //stop node
            if (esNode != null) {
                esNode.stop();
            }
        } catch (Exception e) {
        }
        client = null;
        esNode = null;
    }

    private void connect() {
        Settings localSettings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", false)
                .put("number_of_shards", 1)
                .put("number_of_replicas", 1).build();
        //configure Elasticsearch node
        esNode = isLocalInstance()
                ? nodeBuilder().settings(localSettings).local(true).build().start()
                : nodeBuilder().clusterName(clusterName).node();

        //intialize communication
        client = esNode.client();
    }

    /**
     * Gets the name of the Elasticsearch cluster.
     *
     * @return the name of the cluster.
     */
    public String getClusterName() {
        return clusterName;
    }

    private NodeBuilder nodeBuilder() {
        return NodeBuilder.nodeBuilder();
    }

    private boolean isLocalInstance() {
        return clusterName.equalsIgnoreCase(LOCAL);
    }

    @Override
    protected void postShutdown() {
        logger().debug("Shutting down the elasticsearch index provider '{0}' in repository '{1}'", getName(), getRepositoryName());
        try {
            client.close();
            esNode.stop();
        } catch (Exception e) {
        }
    }

    @Override
    public void validateProposedIndex(ExecutionContext context,
            IndexDefinition defn,
            NodeTypes.Supplier nodeTypeSupplier,
            Problems problems) {
        // first perform some custom validations
    }

    @Override
    protected ManagedIndexBuilder getIndexBuilder(IndexDefinition defn,
            String workspaceName,
            NodeTypes.Supplier nodeTypesSupplier,
            ChangeSetAdapter.NodeTypePredicate matcher) {
        return EsManagedIndexBuilder.create(esNode, client, context(), defn, nodeTypesSupplier, workspaceName, matcher);
    }

    @Override
    protected IndexUsage evaluateUsage(QueryContext context, final IndexCostCalculator calculator, final IndexDefinition defn) {
        return new IndexUsage(context, calculator, defn) {
            @Override
            protected boolean applies(ChildCount operand) {
                // nothing to do about this...
                return false;
            }

            @Override
            protected boolean applies(DynamicOperand operand) {
                if (IndexDefinition.IndexKind.TEXT == defn.getKind() && !(operand instanceof FullTextSearch)) {
                    // text indexes only support FTS operands...
                    return false;
                }
                return super.applies(operand);
            }

            @Override
            protected boolean indexAppliesTo(Or or) {
                boolean appliesToConstraints = super.indexAppliesTo(or);
                if (!appliesToConstraints) {
                    return false;
                }
                Collection<JoinCondition> joinConditions = calculator.joinConditions();
                if (joinConditions.isEmpty()) {
                    return true;
                }
                for (JoinCondition joinCondition : joinConditions) {
                    if (joinCondition instanceof ChildNodeJoinCondition || joinCondition instanceof DescendantNodeJoinCondition) {
                        // the index can't handle OUTER JOINS with OR criteria (see https://issues.jboss.org/browse/MODE-2054)
                        // so reject it, making the query engine fallback to the default behavior which works
                        return false;
                    }
                }
                return true;
            }
        };
    }
}
