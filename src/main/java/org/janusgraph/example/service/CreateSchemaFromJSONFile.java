package org.janusgraph.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.PropertyKeyMaker;

import java.io.File;
import java.io.IOException;

public class CreateSchemaFromJSONFile {

    public static JanusGraph generateSchema(JanusGraph graph, String filePath) throws IOException, ClassNotFoundException {

        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode schemaNode = objectMapper.readTree(new File(filePath));

        // Create the schema
        JanusGraphManagement management = graph.openManagement();

        // Create Vertex Labels
        for (JsonNode vertexLabelNode : schemaNode.get("vertexLabels")) {
            String label = vertexLabelNode.get("name").asText();
            management.makeVertexLabel(label).make();
        }

        // Create Edge Labels
        for (JsonNode edgeLabelNode : schemaNode.get("edgeLabels")) {
            String label = edgeLabelNode.get("name").asText();
            boolean directed = edgeLabelNode.get("directed").asBoolean();
            String multiplicity = edgeLabelNode.get("multiplicity").asText();

            EdgeLabelMaker edgeLabelmaker = management.makeEdgeLabel(label);
//            Need to check the directed and Multiplicity part
            if (directed) {
                edgeLabelmaker.directed();
            }

            if (multiplicity.equals("MULTI")){
                edgeLabelmaker.multiplicity(Multiplicity.MULTI).make();
            }
            else if (multiplicity.equals("MANY2ONE")){
                edgeLabelmaker.multiplicity(Multiplicity.MANY2ONE).make();
            }
            else if (multiplicity.equals("ONE2MANY")){
                edgeLabelmaker.multiplicity(Multiplicity.ONE2MANY).make();
            }
            else if (multiplicity.equals("ONE2ONE")){
                edgeLabelmaker.multiplicity(Multiplicity.ONE2ONE).make();
            }
            else{
                edgeLabelmaker.multiplicity(Multiplicity.SIMPLE).make();
            }
        }


        // Create Property Keys
        for (JsonNode propertyKeyNode : schemaNode.get("propertyKeys")) {
            String label = propertyKeyNode.get("name").asText();
            String dataType = propertyKeyNode.get("dataType").asText();
            String cardinality = propertyKeyNode.get("cardinality").asText();
            Class<?> dt = Class.forName("java.lang." + dataType);
            PropertyKeyMaker propertyKeyMaker = management.makePropertyKey(label).dataType(dt);

            if (cardinality.equals("SINGLE")) {
                propertyKeyMaker.cardinality(Cardinality.SINGLE).make();
            } else if (cardinality.equals("LIST")){
                propertyKeyMaker.cardinality(Cardinality.LIST).make();
            }
            else{
                propertyKeyMaker.cardinality(Cardinality.SET).make();
            }

        }


        // Create Composite Indices
        JsonNode compositeIndices = schemaNode.get("graphIndices").get("compositeIndices");
        for (JsonNode indexNode : compositeIndices) {
            String indexName = indexNode.get("indexName").asText();
            String elementType = indexNode.get("elementType").asText();
            boolean unique = indexNode.get("unique").asBoolean();

            JanusGraphManagement.IndexBuilder indexBuilder = management.buildIndex(indexName, elementType.equals("vertex") ? Vertex.class : Edge.class);
            for (JsonNode propertyKeyNode : indexNode.get("propertyKeys")) {
                String propertyKey = propertyKeyNode.asText();
                indexBuilder.addKey(management.getPropertyKey(propertyKey));
            }

            if (unique) {
                indexBuilder.unique();
            }
            indexBuilder.buildCompositeIndex();
        }

        management.commit();

        return graph;

    }

}
