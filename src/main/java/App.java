import com.opencsv.CSVReader;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class App {

    public static void main(String[] args) {

        final String resourcesDirectory = new File(
                Paths.get("src", "main","resources").toString()
        ).getAbsolutePath();
        final String ZIP_FILE = Paths.get(
                resourcesDirectory,
                "the-marvel-universe-social-network.zip"
        ).toString();
        final String NODES = Paths.get(resourcesDirectory, "nodes.csv").toString();
        final String EDGES = Paths.get(resourcesDirectory, "edges.csv").toString();
        final String NETWORK = Paths.get(resourcesDirectory, "hero-network.csv").toString();
        final String OUTPUT = Paths.get(resourcesDirectory, "marvel.graphml").toString();

        //Unzip files
        System.out.print("Unzipping files...");
        try {
            ZipFile zipFile = new ZipFile(ZIP_FILE);
            zipFile.extractAll(resourcesDirectory);
        } catch (ZipException e) {
            e.printStackTrace();
        }
        System.out.println("OK");

        //Open the graph and the traversal
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = graph.traversal();

        //Read csv
        System.out.print("Reading nodes...");
        List<String[]> nodes = readCsvLineTuple2(NODES);

        //Create first plain string list
        List<String> heroList1 = nodes.stream()
                .filter( x -> x[1].equals("hero") )
                .map( x -> x[0])
                .collect(Collectors.toList());
        List<String> comicList1 = nodes.stream()
                .filter( x -> x[1].equals("comic") )
                .map( x -> x[0])
                .collect(Collectors.toList());
        System.out.println("OK");

        //Print lists numbers
        System.out.println("Total nodes: " + nodes.size());
        System.out.println("Heroes: " + heroList1.size());
        System.out.println("Comics: " + comicList1.size());

        //Insert vertex and map to list
        System.out.print("Inserting nodes...");
        List<Vertex> heroVertexList = heroList1.stream()
                .map( h -> g.addV("hero").property("name", h).next() )
                .collect(Collectors.toList());

        List<Vertex> comicVertexList = comicList1.stream()
                .map( c -> g.addV("comic").property("name", c).next() )
                .collect(Collectors.toList());
        System.out.println("OK");

        //Read edges from csv
        System.out.print("Reading edges...");
        List<String[]> edges = readCsvLineTuple2(EDGES);
        System.out.println("OK");
        System.out.println("Edges: " + edges.size());

        //Create second plain string list
        List<String> heroList2 = edges.stream()
                .map( x -> x[0] )
                .collect(Collectors.toList());
        List<String> comicList2 = edges.stream()
                .map( x -> x[1] )
                .collect(Collectors.toList());

        //Check edges
        System.out.print("Checking edges...");
        List<String> heroesNotContained = heroList2
                .parallelStream()
                .filter( h -> !heroList1.contains(h) )
                .collect(Collectors.toList());
        List<String> comicsNotContained = comicList2
                .parallelStream()
                .filter( c -> !comicList1.contains(c) )
                .collect(Collectors.toList());
        if ( heroesNotContained.size() == 0 && comicsNotContained.size() ==0 ) {
            System.out.println("OK");
        } else {
            System.out.println("FAIL: Some edges contains incorrect nodes");
            System.exit(1);
        }

        //Insert edges in the graph
        System.out.print("Inserting edges...");
        edges.parallelStream().forEach( e -> {
            String currentHero = e[0];
            String currentComic = e[1];
            int heroIndex = heroList1.indexOf(currentHero);
            int comicIndex = comicList1.indexOf(currentComic);

            Vertex hero = heroVertexList.get(heroIndex);
            Vertex comic = comicVertexList.get(comicIndex);

            g.addE("appeared-in").from(hero).to(comic).next();
        });
        System.out.println("OK");

        //Print graph summary
        System.out.println("Graph summary: " + graph);

        //Read network edges csv
        System.out.print("Reading network edges...");
        List<String[]> heroNetworkList = readCsvLineTuple2(NETWORK);
        System.out.println("OK");

        System.out.println("Network edges: " + heroNetworkList.size());

        //Get the 2 parts
        List<String> part1 = heroNetworkList.stream().map( x -> x[0]).collect(Collectors.toList());
        List<String> part2 = heroNetworkList.stream().map( x -> x[1]).collect(Collectors.toList());

        //Aggregate the two lists in part1
        part1.addAll(part2);

        //Check network edges
        System.out.print("Checking network edges...");
        List<String> notContainedInNetwork = part1
                .parallelStream()
                .filter( h -> !heroList1.contains(h) )
                .collect(Collectors.toList());
        if ( notContainedInNetwork.size() == 0 ) {
            System.out.println("OK");
        } else {
            System.out.println("FAIL: Some edges contains incorrect nodes");
            System.exit(1);
        }

        //Insert network edges
        System.out.print("Inserting network edges...");
        heroNetworkList.parallelStream().forEach( e -> {
            String currentHero1 = e[0];
            String currentHero2 = e[1];
            int heroIndex1 = heroList1.indexOf(currentHero1);
            int heroIndex2 = heroList1.indexOf(currentHero2);

            Vertex hero1 = heroVertexList.get(heroIndex1);
            Vertex hero2 = heroVertexList.get(heroIndex2);

            g.addE("appeared-with").from(hero1).to(hero2).next();
        });
        System.out.println("OK");

        //Print graph summary
        System.out.println("Graph summary: " + graph);

        //Print vertex summary
        System.out.println("Vertex summary: " + g.V().groupCount().by(T.label).next());

        //Print edges summary
        System.out.println("Edges summary: " + g.E().groupCount().by(T.label).next());

        //Write graph
        writeGraph(graph, OUTPUT);

    }

    private static List<String[]> readCsvLineTuple2(String filepath) {
        List<String[]> result = new LinkedList<>();

        try {
            CSVReader reader = new CSVReader(new FileReader(filepath));

            String[] nextline;

            //Skip the header
            reader.readNext();

            while ( (nextline = reader.readNext()) != null) {
                String[] trimmed = {
                        nextline[0].trim(),
                        nextline[1].trim()
                };
                result.add(trimmed);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return  result;
    }

    private static void writeGraph(Graph graph, String filepath) {
        GraphMLWriter writer = GraphMLWriter.build().normalize(true).create();
        System.out.println();
        System.out.print("Writing graph...");
        try {
            writer.writeGraph(
                    new FileOutputStream(filepath),
                    graph);

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("OK");
    }

}