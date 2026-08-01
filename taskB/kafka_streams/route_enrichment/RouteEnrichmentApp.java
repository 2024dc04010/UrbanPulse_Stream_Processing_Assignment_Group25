import com.fasterxml.jackson.databind.JsonNode;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;

import org.apache.kafka.streams.StreamsBuilder;

import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.Consumed;

import org.apache.kafka.streams.kstream.KStream;

import org.apache.kafka.streams.kstream.KTable;

import org.apache.kafka.streams.kstream.Produced;



import java.util.Properties;



public class RouteEnrichmentApp {



    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String BUS_GPS_TOPIC = "urbanpulse.bus_gps";

    private static final String ROUTE_SCHEDULE_TOPIC = "urbanpulse.route_schedule";

    private static final String ENRICHED_OUTPUT_TOPIC = "urbanpulse.enriched_bus_gps";



    private static final ObjectMapper mapper = new ObjectMapper();



    public static void main(String[] args) {



        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "urbanpulse-route-enrichment-streams-app");

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());



        StreamsBuilder builder = new StreamsBuilder();



        KStream<String, String> busGpsStream = builder.stream(

                BUS_GPS_TOPIC,

                Consumed.with(Serdes.String(), Serdes.String())

        );



        KTable<String, String> routeScheduleTable = builder.table(

                ROUTE_SCHEDULE_TOPIC,

                Consumed.with(Serdes.String(), Serdes.String())

        );



        KStream<String, String> enrichedStream = busGpsStream.leftJoin(

                routeScheduleTable,

                (busGpsJson, routeJson) -> enrichBusGps(busGpsJson, routeJson)

        );



        enrichedStream.to(

                ENRICHED_OUTPUT_TOPIC,

                Produced.with(Serdes.String(), Serdes.String())

        );



        KafkaStreams streams = new KafkaStreams(builder.build(), props);



        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));



        System.out.println("Kafka Streams route enrichment application started.");

        System.out.println("Joining urbanpulse.bus_gps with urbanpulse.route_schedule KTable.");

        System.out.println("Output topic: urbanpulse.enriched_bus_gps");



        streams.start();

    }



    private static String enrichBusGps(String busGpsJson, String routeJson) {

        try {

            ObjectNode busGpsNode = (ObjectNode) mapper.readTree(busGpsJson);



            if (routeJson != null) {

                JsonNode routeNode = mapper.readTree(routeJson);



                busGpsNode.put("route_name", routeNode.get("route_name").asText());

                busGpsNode.put("terminal", routeNode.get("terminal").asText());

                busGpsNode.put("scheduled_arrival_time", routeNode.get("scheduled_arrival_time").asText());

            } else {

                busGpsNode.put("route_name", "UNKNOWN");

                busGpsNode.put("terminal", "UNKNOWN");

                busGpsNode.put("scheduled_arrival_time", "UNKNOWN");

            }



            return mapper.writeValueAsString(busGpsNode);



        } catch (Exception e) {

            return "{\"error\":\"ENRICHMENT_FAILED\"}";

        }

    }

}
