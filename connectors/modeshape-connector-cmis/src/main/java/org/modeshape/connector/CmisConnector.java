/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.modeshape.connector;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.*;
import javax.jcr.RepositoryException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.nodetype.*;
import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.api.Property;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.bindings.spi.StandardAuthenticationProvider;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.RepositoryInfo;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;
import org.infinispan.schematic.document.Binary;
import org.infinispan.schematic.document.Document;
import org.infinispan.schematic.document.Document.Field;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.federation.spi.Connector;
import org.modeshape.jcr.federation.spi.DocumentChanges;
import org.modeshape.jcr.federation.spi.DocumentChanges.PropertyChanges;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.value.*;
import org.w3c.dom.Element;

/**
 *
 * @author kulikov
 */
public class CmisConnector extends Connector {
    private static final String CMIS_CND_PATH = "org/modeshape/connector/cmis.cnd";

    //path and id for the repository node
    private static final String REPOSITORY_ID = "repository";
    private static final String REPOSITORY_NODE_NAME = "repository";

    private Session session;
    private ValueFactories factories;

    //binding parameters
    private String aclService;
    private String discoveryService;
    private String multifilingService;
    private String navigationService;
    private String objectService;
    private String policyService;
    private String relationshipService;
    private String repositoryService;
    private String versioningService;

    //repository id
    private String repositoryId;

    private PropertyConvertor propertyConvertor = new PropertyConvertor();
    private PropertyMap propertyMap;

    public CmisConnector() {
        super();
    }

    @Override
    public void initialize(NamespaceRegistry registry,
            NodeTypeManager nodeTypeManager) throws RepositoryException, IOException {
        super.initialize(registry, nodeTypeManager);
        this.factories = getContext().getValueFactories();
        propertyMap = new PropertyMap(getContext().getValueFactories());
        
        // default factory implementation
        Map<String, String> parameter = new HashMap<String, String>();

        // user credentials
        //parameter.put(SessionParameter.USER, user);
        //parameter.put(SessionParameter.PASSWORD, passw);

        // connection settings
        parameter.put(SessionParameter.BINDING_TYPE, BindingType.WEBSERVICES.value());
        parameter.put(SessionParameter.WEBSERVICES_ACL_SERVICE, aclService);
        parameter.put(SessionParameter.WEBSERVICES_DISCOVERY_SERVICE, discoveryService);
        parameter.put(SessionParameter.WEBSERVICES_MULTIFILING_SERVICE, multifilingService);
        parameter.put(SessionParameter.WEBSERVICES_NAVIGATION_SERVICE, navigationService);
        parameter.put(SessionParameter.WEBSERVICES_OBJECT_SERVICE, objectService);
        parameter.put(SessionParameter.WEBSERVICES_POLICY_SERVICE, policyService);
        parameter.put(SessionParameter.WEBSERVICES_RELATIONSHIP_SERVICE, relationshipService);
        parameter.put(SessionParameter.WEBSERVICES_REPOSITORY_SERVICE, repositoryService);
        parameter.put(SessionParameter.WEBSERVICES_VERSIONING_SERVICE, versioningService);
        parameter.put(SessionParameter.REPOSITORY_ID, repositoryId);

        SessionFactoryImpl factory = SessionFactoryImpl.newInstance();
        session = factory.createSession(parameter, null,
                new StandardAuthenticationProvider() {
                    @Override
                    public Element getSOAPHeaders(Object portObject) {
                        //Place headers here
                        return super.getSOAPHeaders(portObject);
                    }
                }, null);
        
        // Register the repository-specific node types ...
        //InputStream cndStream = getClass().getClassLoader().getResourceAsStream(nodeDefinitionFile);
        //nodeTypeManager.registerNodeTypes(cndStream, true);
        registry.registerNamespace(CmisLexicon.Namespace.PREFIX, CmisLexicon.Namespace.URI);
        importTypes(session.getTypeDescendants(null, Integer.MAX_VALUE, true), nodeTypeManager);
        registerRepositoryInfoType(nodeTypeManager);
    }

    @Override
    public Document getDocumentById(String id) {
        //search object by its identifier
        if (id.equals(REPOSITORY_ID)) {
            return repository();
        }

        //handle content node
        if (id.endsWith("-content")) {
            String localId = id.replaceAll("-content", "");
            return content(localId);
        }

        CmisObject cmisObject = session.getObject(id);

        //object does not exist? return null
        if (cmisObject == null) {
            return null;
        }

        //converting CMIS object to JCR node
        switch (cmisObject.getBaseTypeId()) {
            case CMIS_FOLDER :   return folderNode(cmisObject);
            case CMIS_DOCUMENT : return documentNode(cmisObject);
        }

        return null;
    }

    @Override
    public String getDocumentId(String path) {
        return session.getObjectByPath(path).getId();
    }

    @Override
    public boolean removeDocument(String id) {
        CmisObject object = session.getObject(id);
        if (object == null) {
            return false;
        }
        object.delete(true);
        return true;
    }

    @Override
    public boolean hasDocument(String id) {
        return session.getObject(id) != null;
    }

    @Override
    public void storeDocument(Document document) {
        //find object
        String key = document.getString("key");
        CmisObject cmisObject = session.getObject(key);

        //no such object? just exit
        if (cmisObject == null) {
            return;
        }

        //pickup node properties
        Document props = document.getDocument("properties").getDocument(JcrLexicon.Namespace.URI);

        Iterator<Field> list = document.fields().iterator();
        while (list.hasNext()) {
            System.out.println("----> " + list.next());
        }
        switch (cmisObject.getBaseTypeId()) {
            case CMIS_FOLDER :
                break;
            case CMIS_DOCUMENT:
                //store binary content
                ContentStream stream = getContentStream(document);
                if (stream != null) {
                    ((org.apache.chemistry.opencmis.client.api.Document)cmisObject).setContentStream(stream, true);
                }
                break;
        }
        //set binary value and properties
    }

    @Override
    public void updateDocument(DocumentChanges delta) {
        System.out.println("--- UPDATE DOCUMENT---\n" + delta.getDocument());
        Document node = delta.getDocument();
        String id = delta.getDocumentId();

        CmisObject cmisObject = session.getObject(id);
        if (cmisObject == null) {
            //unknown object
            return;
        }

        Document props = delta.getDocument().getDocument("properties");
        HashMap<String, Object> updateProperties = new HashMap();

        System.out.println("Cmis object=" + cmisObject);
        PropertyChanges changes = delta.getPropertyChanges();

        Set<Name> changed = changes.getChanged();
        for (Name n : changed) {
            String jcrName = n.getLocalName();
            Document jcrValues = props.getDocument(n.getNamespaceUri());

            String cmisPropertyName = propertyConvertor.cmisName(n);
            Property cmisProperty = cmisObject.getProperty(cmisPropertyName);

            Object value = propertyConvertor.cmisValue(cmisProperty, jcrName, jcrValues);
            updateProperties.put(cmisPropertyName, value);

            cmisObject.updateProperties(updateProperties);
        }
    }

    @Override
    public boolean isReadonly() {
        return false;
    }

    @Override
    public String newDocumentId(String parentId, Name name, Name primaryType) {
        HashMap<String, Object> params = new HashMap();

        //Creating CMIS folder
        if (primaryType.equals(CmisLexicon.FOLDER)) {
            Folder parent = (Folder) session.getObject(parentId);
            String path = parent.getPath() + "/" + name.getLocalName();

            //minimal set of parameters
            params.put(PropertyIds.OBJECT_TYPE_ID,  "cmis:folder");
            params.put(PropertyIds.NAME, name.getLocalName());
            params.put(PropertyIds.PATH, path);

            Folder folder = parent.createFolder(params);
            return folder.getId();
        }

        //Creating CMIS document
        if (primaryType.equals(CmisLexicon.DOCUMENT)) {
            Folder parent = (Folder) session.getObject(parentId);
            String path = parent.getPath() + "/" + name.getLocalName();

            //minimal set of parameters
            params.put(PropertyIds.OBJECT_TYPE_ID,  "cmis:document");
            params.put(PropertyIds.NAME, name.getLocalName());
            params.put(PropertyIds.PATH, path);

            String id = parent.createDocument(params, null, VersioningState.NONE).getId();
            return id;
        }

        return null;
    }

    /**
     * Translates CMIS folder object to JCR node
     * 
     * @param cmisObject CMIS folder object
     * @return JCR node document.
     */
    private Document folderNode(CmisObject cmisObject) {
        Folder folder = (Folder) cmisObject;
        DocumentWriter writer = newDocument(folder.getId());

        ObjectType objectType = cmisObject.getType();
        if (objectType.isBaseType()) {
            writer.setPrimaryType("nt:folder");
        } else {
            writer.setPrimaryType(objectType.getId());
        }

        writer.setParent(folder.getParentId());
        writer.addMixinType("mix:created");
        writer.addMixinType("mix:lastModified");
        writer.addMixinType("mix:referenceable");

        cmisProperties(folder, writer);
        cmisChildren((Folder)folder, writer);

        //append repository information to the root node
        if (folder.isRootFolder()) {
            writer.addChild(REPOSITORY_ID, REPOSITORY_NODE_NAME);
        }

        return writer.document();
    }

    /**
     * Translates cmis document object to JCR node.
     * 
     * @param cmisObject cmis document node
     * @return JCR node document.
     */
    public Document documentNode(CmisObject cmisObject) {
        org.apache.chemistry.opencmis.client.api.Document doc =
                (org.apache.chemistry.opencmis.client.api.Document) cmisObject;
        DocumentWriter writer = newDocument(doc.getId());

        ObjectType objectType = cmisObject.getType();
        if (objectType.isBaseType()) {
            writer.setPrimaryType(NodeType.NT_FILE);
        } else {
            writer.setPrimaryType(objectType.getId());
        }

        writer.addMixinType("mix:created");
        writer.addMixinType("mix:lastModified");
        writer.addMixinType("mix:referenceable");

        cmisProperties(doc, writer);
        writer.addChild(doc.getId() + "-content", "jcr:content");
        
        return writer.document();
    }

    private Document content(String id) {
        DocumentWriter writer = newDocument(id + "-content");

        org.apache.chemistry.opencmis.client.api.Document doc =
                (org.apache.chemistry.opencmis.client.api.Document)session.getObject(id);
        writer.setPrimaryType("nt:resource");
        
        if (doc.getContentStream() != null) {
            InputStream is = doc.getContentStream().getStream();
            BinaryValue content = factories.getBinaryFactory().create(is);
            writer.addProperty(JcrConstants.JCR_DATA, content);
            //writer.addProperty(CmisLexicon.FILE_NAME, doc.getContentStream().getFileName());
            writer.addProperty(CmisLexicon.MIME_TYPE, doc.getContentStream().getMimeType());
        }

        return writer.document();
    }

    /**
     * Converts CMIS object's properties to JCR node properties.
     *
     * @param object CMIS object
     * @param writer JCR node representation.
     */
    private void cmisProperties(CmisObject object, DocumentWriter writer) {
        //convert properties
        List<Property<?>> list = object.getProperties();
        for (Property property : list) {
            String pname = propertyMap.findJcrName(property.getId());
            writer.addProperty(pname, propertyMap.jcrValues(property));
        }
    }


    /**
     * Converts CMIS folder children to JCR node children
     * 
     * @param folder CMIS folder
     * @param writer JCR node representation
     */
    private void cmisChildren(Folder folder, DocumentWriter writer) {
        ItemIterable<CmisObject> it = folder.getChildren();
        for (CmisObject obj : it) {
            writer.addChild(obj.getId(), obj.getName());
        }
    }

    /**
     * Translates CMIS repository information into Node.
     * 
     * @return node document.
     */
    private Document repository() {
        RepositoryInfo info = session.getRepositoryInfo();
        DocumentWriter writer = newDocument(REPOSITORY_ID);

        writer.setPrimaryType(CmisLexicon.REPOSITORY);
        writer.setId(REPOSITORY_ID);

        //product name/vendor/version
        writer.addProperty(CmisLexicon.VENDOR_NAME, info.getVendorName());
        writer.addProperty(CmisLexicon.PRODUCT_NAME, info.getProductName());
        writer.addProperty(CmisLexicon.PRODUCT_VERSION, info.getProductVersion());
        
        return writer.document();
    }

    /**
     * Creates content stream using JCR node.
     *
     * @param document JCR node representation
     * @return CMIS content stream object
     */
    private ContentStream getContentStream(Document document) {
        //pickup node properties
        Document props = document.getDocument("properties").getDocument(CmisLexicon.Namespace.URI);

        Iterator<Field> list = props.fields().iterator();
        while (list.hasNext()) {
            System.out.println("===> " + list.next());
        }

        //extract binary value and content
        Binary value = props.getBinary("data");

        if (value == null) {
            return null;
        }

        byte[] content = value.getBytes();
        String fileName = props.getString("fileName");
        String mimeType = props.getString("mimeType");

        //wrap with input stream
        ByteArrayInputStream bin = new ByteArrayInputStream(content);
        bin.reset();

        //create content stream
        ContentStreamImpl contentStream = new ContentStreamImpl(fileName, BigInteger.valueOf(content.length), mimeType, bin);

        return contentStream;
    }

    /**
     * Import CMIS types to JCR repository.
     *
     * @param types CMIS types
     * @param typeManager JCR type manager
     */
    private void importTypes(List<Tree<ObjectType>> types, NodeTypeManager typeManager) throws RepositoryException {
        for (int i = 0; i < types.size(); i++) {
            Tree<ObjectType> tree = types.get(i);
            importType(tree.getItem(), typeManager);
            importTypes(tree.getChildren(), typeManager);
        }
    }

    /**
     * Import given CMIS type to the JCR repository.
     *
     * @param cmisType cmis object type
     * @param typeManager JCR type manager/
     */
    public void importType(ObjectType cmisType, NodeTypeManager typeManager) throws RepositoryException {
        //skip base types because we are going to
        //map base types directly
        if (cmisType.isBaseType()) {
            return;
        }

        //create node type template
        NodeTypeTemplate type = typeManager.createNodeTypeTemplate();

        //convert CMIS type's attributes to node type template we have just created
        type.setName(cmisType.getId());
        type.setAbstract(false);
        type.setMixin(true);
        type.setOrderableChildNodes(true);
        type.setQueryable(true);
        type.setDeclaredSuperTypeNames(superTypes(cmisType));

        Map<String, PropertyDefinition<?>> props = cmisType.getPropertyDefinitions();
        Set<String> names = props.keySet();

        //properties
        for (String name : names) {
            PropertyDefinition pd = props.get(name);
            PropertyDefinitionTemplate pt = typeManager.createPropertyDefinitionTemplate();

            pt.setAutoCreated(false);
            pt.setAvailableQueryOperators(new String[]{});
            pt.setName(name);
            pt.setMandatory(pd.isRequired());

            type.getPropertyDefinitionTemplates().add(pt);
        }

        //register type
        NodeTypeDefinition[] nodeDefs = new NodeTypeDefinition[] {type};
        typeManager.registerNodeTypes(nodeDefs, true);
    }

    /**
     * Determines supertypes for the given CMIS type in terms of JCR.
     *
     * @param cmisType given CMIS type
     * @return supertypes in JCR lexicon.
     */
    private String[] superTypes(ObjectType cmisType) {
        if (cmisType.getBaseTypeId() == BaseTypeId.CMIS_FOLDER) {
            return new String[] {"nt:folder"};
        }

        if (cmisType.getBaseTypeId() == BaseTypeId.CMIS_DOCUMENT) {
            return new String[] {"nt:file"};
        }

        return new String[]{cmisType.getBaseTypeId().name()};
    }

    /**
     * Defines node type for the repository info.
     *
     * @param typeManager JCR node type manager.
     * @throws RepositoryException
     */
    private void registerRepositoryInfoType(NodeTypeManager typeManager) throws RepositoryException {
        //create node type template
        NodeTypeTemplate type = typeManager.createNodeTypeTemplate();

        //convert CMIS type's attributes to node type template we have just created
        type.setName("cmis:repository");
        type.setAbstract(false);
        type.setMixin(true);
        type.setOrderableChildNodes(true);
        type.setQueryable(true);
        type.setDeclaredSuperTypeNames(new String[]{"nt:folder"});

        PropertyDefinitionTemplate vendorName = typeManager.createPropertyDefinitionTemplate();
        vendorName.setAutoCreated(false);
        vendorName.setName("cmis:vendorName");
        vendorName.setMandatory(false);

        type.getPropertyDefinitionTemplates().add(vendorName);

        PropertyDefinitionTemplate productName = typeManager.createPropertyDefinitionTemplate();
        productName.setAutoCreated(false);
        productName.setName("cmis:productName");
        productName.setMandatory(false);

        type.getPropertyDefinitionTemplates().add(productName);

        PropertyDefinitionTemplate productVersion = typeManager.createPropertyDefinitionTemplate();
        productVersion.setAutoCreated(false);
        productVersion.setName("cmis:productVersion");
        productVersion.setMandatory(false);

        type.getPropertyDefinitionTemplates().add(productVersion);

        //register type
        NodeTypeDefinition[] nodeDefs = new NodeTypeDefinition[] {type};
        typeManager.registerNodeTypes(nodeDefs, true);
    }
}
