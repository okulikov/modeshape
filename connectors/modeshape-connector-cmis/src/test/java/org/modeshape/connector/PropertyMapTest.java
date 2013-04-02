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

import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author kulikov
 */
public class PropertyMapTest {

    private Properties pmap = new Properties(null);

    public PropertyMapTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of findJcrName method, of class PropertyMap.
     */
    @Test
    public void testFindJcrName() {
        assertEquals("jcr:uuid", pmap.findJcrName("cmis:objectId"));
        assertEquals("cmis:custom", pmap.findJcrName("cmis:custom"));
    }

    /**
     * Test of findCmisName method, of class PropertyMap.
     */
    @Test
    public void testFindCmisName() {
        assertEquals("cmis:objectId", pmap.findCmisName("jcr:uuid"));
        assertEquals("jcr:custom", pmap.findCmisName("jcr:custom"));
    }
}
