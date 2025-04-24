package org.janusgraph.example;

import org.janusgraph.core.*;
import org.janusgraph.example.service.CalculatePerformane;
import org.janusgraph.example.service.CreateSchemaFromJSONFile;
import org.janusgraph.example.service.LoadDataFromFile;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

public class sampleGraphApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        String backend = "foundationdb";

        JanusGraph graph = JanusGraphFactory.open("src/main/resources/janusgraph-"+backend+".properties");

        CalculatePerformane performance = new CalculatePerformane();

        long schemaStartTime = System.nanoTime();

        CreateSchemaFromJSONFile createSchema = new CreateSchemaFromJSONFile();
        graph = createSchema.generateSchema(graph, "src/main/resources/janusgraphSchema.json");

        long schemaEndTime = System.nanoTime();
        double schemaLoadTime = performance.calculateTimeInSeconds(schemaStartTime, schemaEndTime);
        System.out.println("Time taken to generate schema: " + schemaLoadTime + " seconds");

        LoadDataFromFile loader = new LoadDataFromFile();

        long nodesStartTime = System.nanoTime();

        graph = loader.loadNodes(graph, "src/main/resources/air-routes-latest-nodes.csv");

        long nodesEndTime = System.nanoTime();
        double nodesLoadTime = performance.calculateTimeInSeconds(nodesStartTime, nodesEndTime);
        System.out.println("Time taken to load nodes: " + nodesLoadTime + " seconds");
        System.out.println("Number of vertices processed: " + graph.traversal().V().count().next());


        long edgesStartTime = System.nanoTime();

        graph = loader.loadEdges(graph, "src/main/resources/air-routes-latest-edges.csv");

        long edgesEndTime = System.nanoTime();
        double edgesLoadTime = performance.calculateTimeInSeconds(edgesStartTime, edgesEndTime);
        System.out.println("Time taken to load edges: " + edgesLoadTime + " seconds");
        System.out.println("Number of edges processed: " + graph.traversal().E().count().next());

        if (!backend.equals("inmemory")) {
            Path dbDirectory = Paths.get("/db/" + backend);
            long totalSizeInBytes = performance.calculateDirectorySize(dbDirectory.toFile());
            double totalSizeInMB = totalSizeInBytes / 1_048_576.0;  // Convert to MB
            System.out.println("Database size: " + totalSizeInMB + " MB");
        }

        graph.close();
    }

}
