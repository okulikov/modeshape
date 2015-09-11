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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import javax.jcr.query.qom.And;
import javax.jcr.query.qom.BindVariableValue;
import javax.jcr.query.qom.Constraint;
import javax.jcr.query.qom.DynamicOperand;
import javax.jcr.query.qom.LowerCase;
import javax.jcr.query.qom.NodeLocalName;
import javax.jcr.query.qom.NodeName;
import javax.jcr.query.qom.Not;
import javax.jcr.query.qom.Or;
import javax.jcr.query.qom.PropertyExistence;
import javax.jcr.query.qom.PropertyValue;
import javax.jcr.query.qom.StaticOperand;
import javax.jcr.query.qom.UpperCase;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.ModeShapeLexicon;
import org.modeshape.jcr.api.query.qom.Between;
import org.modeshape.jcr.api.query.qom.SetCriteria;
import org.modeshape.jcr.query.model.Comparison;
import org.modeshape.jcr.query.model.FullTextSearch;
import org.modeshape.jcr.query.model.Length;
import org.modeshape.jcr.query.model.Literal;
import org.modeshape.jcr.query.model.NodeDepth;
import org.modeshape.jcr.query.model.NodePath;
import org.modeshape.jcr.value.ValueFactories;

/**
 *
 * @author kulikov
 */
public class Operations {

    private final ValueFactories valueFactories;
    private final EsIndexColumns columns;
    
    /**
     * 
     * @param columns 
     */
    public Operations(ValueFactories valueFactories, EsIndexColumns columns) {
        this.valueFactories = valueFactories;
        this.columns = columns;
    }
    
    public FilterBuilder createFilter(Collection<Constraint> constraints, Map<String, Object> variables) {
        if (constraints.isEmpty()) {
            return FilterBuilders.matchAllFilter();
        }
        
        BoolFilterBuilder builder = FilterBuilders.boolFilter();
        for (Constraint c : constraints) {
            builder = builder.must(build(c, variables));
        }
        
        return builder;
    }

    public FilterBuilder build(Constraint constraint, Map<String, Object> variables) {
        if (constraint instanceof Between) {
            Between between = (Between) constraint;
            String field = (String)operand(between.getOperand()).apply(between.getOperand(), variables);
            Object low = operand(between.getLowerBound()).apply(field, between.getLowerBound(), variables);
            Object high = operand(between.getUpperBound()).apply(field, between.getUpperBound(), variables);
            
            return FilterBuilders.rangeFilter(field).from(low).to(high)
                    .includeLower(between.isLowerBoundIncluded())
                    .includeUpper(between.isUpperBoundIncluded());
        }
        
        if (constraint instanceof Or) {
            Or or = (Or)constraint;
            return FilterBuilders.orFilter(
                    build(or.getConstraint1(), variables),
                    build(or.getConstraint2(), variables));
        }

        if (constraint instanceof And) {
            And and = (And)constraint;
            return FilterBuilders.andFilter(
                    build(and.getConstraint1(), variables),
                    build(and.getConstraint2(), variables));
        }

        if (constraint instanceof Not) {
            Not not = (Not)constraint;
            return FilterBuilders.notFilter(build(not.getConstraint(), variables));
        }

        if (constraint instanceof Comparison) {
            Comparison comp = (Comparison)constraint;
            String field = (String) operand(comp.getOperand1()).apply(comp.getOperand1(), variables);
            Object value = operand(comp.getOperand2()).apply(field, comp.getOperand2(), variables);
            switch (comp.operator()) {
                case EQUAL_TO:
                    return FilterBuilders.queryFilter(QueryBuilders.matchQuery(field, value));
                case GREATER_THAN:
                    return FilterBuilders.rangeFilter(field).gt(value);
                case GREATER_THAN_OR_EQUAL_TO:
                    return FilterBuilders.rangeFilter(field).gte(value);
                case LESS_THAN:
                    return FilterBuilders.rangeFilter(field).lt(value);
                case LESS_THAN_OR_EQUAL_TO:
                    return FilterBuilders.rangeFilter(field).lte(value);
                case NOT_EQUAL_TO:
                    return FilterBuilders.notFilter(
                            FilterBuilders.rangeFilter(field).from(value).to(value));
                case LIKE:
                    String queryString = (String)value;                    
                    
                    if (!queryString.contains("%")) {
                        return FilterBuilders.queryFilter(QueryBuilders.queryStringQuery(queryString));
                    }
                    
                    String[] terms = queryString.split(" ");
                    FilterBuilder[] termFilters = new FilterBuilder[terms.length];
                    
                    for (int i = 0; i < termFilters.length; i++) {
                        termFilters[i] = terms[i].contains("%") ?
                                FilterBuilders.queryFilter(QueryBuilders.wildcardQuery(field, terms[i].replaceAll("%", "*"))) :
                                FilterBuilders.queryFilter(QueryBuilders.queryStringQuery(terms[i]));
                    }
                    
                    if (termFilters.length == 1) {
                        return termFilters[0];
                    }
                    
                    FilterBuilder builder = 
                            FilterBuilders.andFilter(termFilters[0], termFilters[1]);
                    for (int i = 2; i < termFilters.length; i++) {
                        builder = FilterBuilders.andFilter(builder, termFilters[i]);
                    }
                    
                    return builder;
            }
        }

        if (constraint instanceof SetCriteria) {
            SetCriteria setCriteria = (SetCriteria)constraint;
            
            String field = (String) operand(setCriteria.getOperand()).apply(setCriteria.getOperand(), variables);
            ArrayList list = new ArrayList();
            for (StaticOperand so : setCriteria.getValues()) {
                Object vals = operand(so).apply(field, so, variables);
                if (vals instanceof Object[]) {
                    list.addAll(Arrays.asList((Object[]) vals));
                } else if (vals != null) {
                    list.add(vals);
                }
            }
            
            Object[] set = new Object[list.size()];
            list.toArray(set);
            return FilterBuilders.inFilter(field, set);
        }
        

        if (constraint instanceof PropertyExistence) {
            PropertyExistence pe = (PropertyExistence)constraint;
            return FilterBuilders.existsFilter(pe.getPropertyName());
        }
        
        if (constraint instanceof FullTextSearch) {
            FullTextSearch fts = (FullTextSearch) constraint;
            return FilterBuilders.queryFilter(
                    QueryBuilders.queryStringQuery(fts.fullTextSearchExpression()));
        }
        
        return null;
    }
    
    
    private abstract static class OperandBuilder<T> {
        public abstract Object apply(T operand, Map<String, Object> variables);
    }
    

    private static abstract class StaticOperandBuilder<T> {
        public abstract Object apply(String field, T operand, Map<String, Object> variables);
    }
    
    
    private OperandBuilder operand(DynamicOperand op) {
        if (op instanceof PropertyValue) {
            return PROPERTY_VALUE_BUILDER;
        } 
        
        if (op instanceof NodePath) {
            return PATH_BUILDER;
        } 
        
        if (op instanceof NodeDepth) {
            return DEPTH_BUILDER;
        } 
        
        if (op instanceof NodeName) {
            return NODE_NAME_BUILDER;
        } 
        
        if (op instanceof LowerCase) {
            return LOWER_CASE_BUILDER;
        }
        
        if (op instanceof UpperCase) {
            return UPPER_CASE_BUILDER;
        }

        if (op instanceof NodeLocalName) {
            return NODE_LOCALNAME_BUILDER;
        }

        if (op instanceof Length) {
            return LENGTH_BUILDER;
        }
        
        return null;
    }
 
    private StaticOperandBuilder operand(StaticOperand op) {
        if (op instanceof Literal) {
            return LITERAL_BUILDER;
        }
        
        if (op instanceof BindVariableValue) {
            return BIND_VARIABLE_VALUE_BUILDER;
        }
        
        return null;
    }
    
    private final OperandBuilder PROPERTY_VALUE_BUILDER = new OperandBuilder<PropertyValue>() {
        @Override
        public String apply(PropertyValue operand, Map<String, Object> variables) {
            return operand.getPropertyName();
        }
    };

    private final StaticOperandBuilder LITERAL_BUILDER = new StaticOperandBuilder<Literal>() {
        @Override
        public Object apply(String field, Literal op, Map<String, Object> variables) {
            EsIndexColumn col = columns.column(field);
            
            if (op.value() instanceof Object[]) {
                return col.columnValue((Object[])col.cast((Object[])op.value()));
            }
                        
            return col.columnValue(col.cast(op.value()));
        }
    };

    private final StaticOperandBuilder BIND_VARIABLE_VALUE_BUILDER = new StaticOperandBuilder<BindVariableValue>() {
        @Override
        public Object apply(String field, BindVariableValue op, Map<String, Object> variables) {
            EsIndexColumn col = columns.column(field);
            Object value = variables.get(op.getBindVariableName());
            
            if (value instanceof StaticOperand) {
                return operand((StaticOperand)value).apply(field, value, variables);
            }
            
            if (value instanceof DynamicOperand) {
                return operand((DynamicOperand)value).apply(value, variables);
            }
            
            if (value instanceof Object[]) {
                return col.cast((Object[]) value);
            }
            
            if (value instanceof Collection) {
                return col.cast((Collection) value);
            }
            
            return col.cast(variables.get(op.getBindVariableName()));
        }
    };

    private final OperandBuilder LOWER_CASE_BUILDER = new OperandBuilder<LowerCase>() {
        @Override
        public Object apply(LowerCase op, Map<String, Object> variables) {
            return EsIndexColumn.LOWERCASE_PREFIX + 
                    ((String)operand(op.getOperand()).apply(op.getOperand(), variables));
        }
    };

    private final OperandBuilder UPPER_CASE_BUILDER = new OperandBuilder<UpperCase>() {
        @Override
        public Object apply(UpperCase op, Map<String, Object> variables) {
            return EsIndexColumn.UPPERCASE_PREFIX + 
                    ((String)operand(op.getOperand()).apply(op.getOperand(), variables));
        }
    };
    
    private final OperandBuilder NODE_LOCALNAME_BUILDER = new OperandBuilder<NodeLocalName>() {
        @Override
        public String apply(NodeLocalName op, Map<String, Object> variables) {
            return valueFactories.getStringFactory().create(ModeShapeLexicon.LOCALNAME);
        }
    };


    private final OperandBuilder NODE_NAME_BUILDER = new OperandBuilder<NodeName>() {
        @Override
        public String apply(NodeName op, Map<String, Object> variables) {
            return valueFactories.getStringFactory().create(JcrLexicon.NAME);
        }
    };

    private final OperandBuilder LENGTH_BUILDER = new OperandBuilder<Length>() {
        @Override
        public String apply(Length op, Map<String, Object> variables) {
            return EsIndexColumn.LENGTH_PREFIX + op.getPropertyValue().getPropertyName();
        }
    };

    private final OperandBuilder PATH_BUILDER = new OperandBuilder<NodePath>() {
        @Override
        public String apply(NodePath op, Map<String, Object> variables) {
            return valueFactories.getStringFactory().create(JcrLexicon.PATH);
        }
    };

    private final OperandBuilder DEPTH_BUILDER = new OperandBuilder<NodeDepth>() {
        @Override
        public String apply(NodeDepth op, Map<String, Object> variables) {
            return valueFactories.getStringFactory().create(ModeShapeLexicon.DEPTH);
        }
    };
    
    
    
}
