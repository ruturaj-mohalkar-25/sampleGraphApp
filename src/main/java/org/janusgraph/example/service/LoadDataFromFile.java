package org.janusgraph.example.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class LoadDataFromFile {

    public static JanusGraph loadNodes(JanusGraph graph, String filePath) throws IOException {
        try (Reader in = new FileReader(filePath)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader().parse(in);
            for (CSVRecord record : records) {
                Integer id = Integer.valueOf(record.get(0));
                if (id > 0) {
                    String label = record.get(1);
                    if (label.equals("airport")) {
                        String type = record.get(2);
                        String code = record.get(3);
                        String icao = record.get(4);
                        String desc = record.get(5);
                        String region = record.get(6);
                        Integer runways = Integer.valueOf(record.get(7));
                        Integer longest = Integer.valueOf(record.get(8));
                        Integer elev = Integer.valueOf(record.get(9));
                        String country = record.get(10);
                        String city = record.get(11);
                        Double lat = Double.valueOf(record.get(12));
                        Double lon = Double.valueOf(record.get(13));

                        Vertex vertex = graph.addVertex(label);
                        vertex.property("identity",record.get(0));
                        vertex.property("type",type);
                        vertex.property("code",code);
                        vertex.property("icao",icao);
                        vertex.property("desc",desc);
                        vertex.property("region",region);
                        vertex.property("runways",runways);
                        vertex.property("longest",longest);
                        vertex.property("elev",elev);
                        vertex.property("country",country);
                        vertex.property("city",city);
                        vertex.property("lat",lat);
                        vertex.property("lon",lon);
                    }
                    else{
                        String type = record.get(2);
                        String code = record.get(3);
                        String desc = record.get(5);

                        Vertex vertex = graph.addVertex(label);
                        vertex.property("identity",record.get(0));
                        vertex.property("type",type);
                        vertex.property("code",code);
                        vertex.property("desc",desc);
                    }
                }
                graph.tx().commit();
            }
            graph.tx().commit(); // Commit the transaction after loading all nodes
        } catch (IOException e) {
            e.printStackTrace();
            graph.tx().rollback();
        }
        return graph;
    }


    public static JanusGraph loadEdges(JanusGraph graph, String filePath) throws IOException {
        try (Reader in = new FileReader(filePath)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader().parse(in);
            for (CSVRecord record : records) {
                String identity = record.get(0);
                String sourceId = record.get(1);
                String targetId = record.get(2);
                String label = record.get(3);
                String dist = record.get(4);

                Vertex source = graph.traversal().V().has("identity",sourceId).next();
                Vertex target = graph.traversal().V().has("identity",targetId).next();
                Edge edge = source.addEdge(label,target);
                edge.property("identity",identity);
                edge.property("dist",dist);

                graph.tx().commit();
            }
            graph.tx().commit(); // Commit the transaction after loading all edges
        } catch (IOException e) {
            e.printStackTrace();
        }
        return graph;
    }


}
