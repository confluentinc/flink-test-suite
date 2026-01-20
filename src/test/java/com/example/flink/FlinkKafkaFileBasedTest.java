package com.example.flink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class FlinkKafkaFileBasedTest {

    private static KafkaContainer kafka;
    private static String bootstrapServers;
    private static boolean useExternalKafka = false;
    
    // Configurable via system properties (required)
    private static String sqlFile;
    private static String inputFile;
    private static String outputFile;
    
    // Configurable via system properties (optional)
    private static String javaJars;
    private static String pythonPath;

    @BeforeAll
    public static void setUp() {
        // Initialize required file paths from system properties
        sqlFile = System.getProperty("sqlFile");
        inputFile = System.getProperty("inputFile");
        outputFile = System.getProperty("outputFile");
        
        // Validate required parameters
        if (sqlFile == null || sqlFile.isEmpty()) {
            throw new IllegalArgumentException("Required parameter 'sqlFile' is not set. Use -DsqlFile=<path>");
        }
        if (inputFile == null || inputFile.isEmpty()) {
            throw new IllegalArgumentException("Required parameter 'inputFile' is not set. Use -DinputFile=<path>");
        }
        if (outputFile == null || outputFile.isEmpty()) {
            throw new IllegalArgumentException("Required parameter 'outputFile' is not set. Use -DoutputFile=<path>");
        }
        
        // Initialize optional parameters (can be null or empty)
        javaJars = System.getProperty("javaJars", "");
        pythonPath = System.getProperty("pythonPath", "");
        
        System.out.println("\n========================================");
        System.out.println("Test Configuration");
        System.out.println("========================================");
        System.out.println("SQL File:    " + sqlFile);
        System.out.println("Input File:  " + inputFile);
        System.out.println("Output File: " + outputFile);
        System.out.println("Java JARs:   " + (javaJars.isEmpty() ? "(none)" : javaJars));
        System.out.println("Python Path: " + (pythonPath.isEmpty() ? "(none)" : pythonPath));
        System.out.println("========================================\n");
        
        // Check for external Kafka (from docker-compose or environment)
        String externalKafka = System.getProperty("kafka.bootstrap.servers", 
                                System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        
        if (externalKafka != null && !externalKafka.isEmpty()) {
            // Use external Kafka (e.g., from docker-compose)
            useExternalKafka = true;
            bootstrapServers = externalKafka;
            System.out.println("Using external Kafka: " + bootstrapServers);
            
            // Wait for Kafka to be ready
            waitForKafka(bootstrapServers, 60);
        } else {
            // Use testcontainers Kafka
            System.out.println("Starting Kafka testcontainer...");
            kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:8.1.1")).withKraft();
            kafka.start();
            bootstrapServers = kafka.getBootstrapServers();
            System.out.println("Kafka testcontainer started: " + bootstrapServers);
        }
    }
    
    /**
     * Wait for Kafka to be ready (for external Kafka)
     */
    private static void waitForKafka(String servers, int timeoutSeconds) {
        System.out.println("Waiting for Kafka to be ready at " + servers + "...");
        
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (timeoutSeconds * 1000L);
        
        while (System.currentTimeMillis() < endTime) {
            try (AdminClient admin = AdminClient.create(props)) {
                admin.listTopics().names().get(10, java.util.concurrent.TimeUnit.SECONDS);
                System.out.println("✓ Kafka is ready!");
                return;
            } catch (Exception e) {
                System.out.println("Kafka not ready yet, retrying... (" + e.getMessage() + ")");
            }
        }
        
        throw new RuntimeException("Kafka not ready after " + timeoutSeconds + " seconds");
    }

    @BeforeEach
    public void resetTopics() throws Exception {
        // Get topics from input and output files
        Set<String> topicsToCreate = getTopicsFromFiles();
        createTopics(topicsToCreate);
    }
    
    /**
     * Extracts unique topics from input.txt, output.txt, and SQL CREATE TABLE statements
     */
    private Set<String> getTopicsFromFiles() throws IOException {
        Set<String> topics = new HashSet<>();
        
        // Get topics from input file
        List<TopicMessage> inputMessages = loadMessagesWithTopicsFromFile(inputFile);
        topics.addAll(getUniqueTopics(inputMessages));
        
        // Get topics from output file
        List<TopicMessage> outputMessages = loadMessagesWithTopicsFromFile(outputFile);
        topics.addAll(getUniqueTopics(outputMessages));
        
        // Get topics from SQL CREATE TABLE statements
        Set<String> sqlTopics = extractTopicsFromSQL(sqlFile);
        topics.addAll(sqlTopics);
        
        System.out.println("Topics to create: " + topics);
        return topics;
    }
    
    /**
     * Extracts topics from CREATE TABLE statements in SQL file.
     * Parses the 'topic' property from WITH clauses after replacing placeholders.
     */
    private Set<String> extractTopicsFromSQL(String sqlFilePath) throws IOException {
        Set<String> topics = new HashSet<>();
        
        System.out.println("Extracting topics from SQL: " + sqlFilePath);
        
        // Load SQL statements
        List<String> sqlStatements = loadSQLFromFile(sqlFilePath);
        
        for (String sql : sqlStatements) {
            // Check if this is a CREATE TABLE statement
            if (sql.trim().toUpperCase().startsWith("CREATE TABLE") || 
                sql.trim().toUpperCase().startsWith("CREATE TEMPORARY TABLE")) {
                
                // Extract topic from WITH clause
                String topic = extractTopicFromCreateTable(sql);
                if (topic != null && !topic.isEmpty()) {
                    topics.add(topic);
                    System.out.println("  Found topic in SQL: " + topic);
                }
            }
        }
        
        return topics;
    }
    
    /**
     * Extracts the 'topic' property value from a CREATE TABLE statement's WITH clause.
     */
    private String extractTopicFromCreateTable(String sql) {
        Map<String, String> properties = extractWithProperties(sql);
        return properties.get("topic");
    }

    @AfterAll
    public static void tearDown() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    private static void createTopics(Set<String> topicNames) throws ExecutionException, InterruptedException {
        
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            List<String> topicsToDelete = new ArrayList<>();
            
            for (String topicName : topicNames) {
                if (existingTopics.contains(topicName)) {
                    topicsToDelete.add(topicName);
                }
            }
            
            if (!topicsToDelete.isEmpty()) {
                adminClient.deleteTopics(topicsToDelete).all().get();
                System.out.println("✓ Deleted existing topics: " + topicsToDelete);
            }
            
            List<NewTopic> topics = new ArrayList<>();
            for (String topicName : topicNames) {
                topics.add(new NewTopic(topicName, 1, (short) 1));
            }
            
            adminClient.createTopics(topics).all().get();
            System.out.println("✓ Topics created: " + topicNames);
        }
    }

    @Test
    public void testFlinkSQLFromFile() throws Exception {
        sendTestData();
        
        TableResult tableResult = runFlinkJobFromFile(sqlFile);
        
        verifyOutput();

        tableResult.getJobClient().ifPresent(jobClient -> {
            try {
                jobClient.cancel().get();
                System.out.println("✓ Job cancelled successfully");
            } catch (Exception e) {
                System.err.println("Failed to cancel job: " + e.getMessage());
            }
        });
    }

    private void sendTestData() throws Exception {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Load messages with topics from input file
        List<TopicMessage> messages = loadMessagesWithTopicsFromFile(inputFile);
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (TopicMessage tm : messages) {
                ProducerRecord<String, String> record = new ProducerRecord<>(tm.topic, tm.message);
                producer.send(record).get();
                System.out.println("Sent to " + tm.topic + ": " + tm.message);
            }
            producer.flush();
        }
        System.out.println("Test data sent");
    }
    
    /**
     * Helper class to store topic and message pairs from input file
     */
    private static class TopicMessage {
        String topic;
        String message;
        
        TopicMessage(String topic, String message) {
            this.topic = topic;
            this.message = message;
        }
    }
    
    /**
     * Loads messages from file in format: topic:message
     * Example: person-input:{"name":"Alice","age":25}
     */
    private List<TopicMessage> loadMessagesWithTopicsFromFile(String filePath) throws IOException {
        System.out.println("Loading messages from: " + filePath);
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        List<TopicMessage> messages = new ArrayList<>();
        
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue; // Skip empty lines and comments
            }
            
            // Parse "topic:message" format
            int colonIndex = line.indexOf(':');
            if (colonIndex == -1) {
                throw new IllegalArgumentException(
                    "Invalid format in input.txt. Expected 'topic:message', got: " + line);
            }
            
            String topic = line.substring(0, colonIndex).trim();
            String message = line.substring(colonIndex + 1).trim();
            
            messages.add(new TopicMessage(topic, message));
        }
        
        System.out.println("Loaded " + messages.size() + " messages across " + 
            getUniqueTopics(messages).size() + " topics");
        return messages;
    }
    
    /**
     * Extracts unique topic names from messages
     */
    private Set<String> getUniqueTopics(List<TopicMessage> messages) {
        Set<String> topics = new HashSet<>();
        for (TopicMessage tm : messages) {
            topics.add(tm.topic);
        }
        return topics;
    }

    private TableResult runFlinkJobFromFile(String sqlFilePath) throws Exception {
        // Load SQL statements first to check for Python UDF
        List<String> sqlStatements = loadSQLFromFile(sqlFilePath);
        
        // Filter out ADD JAR statements from SQL file
        List<String> filteredStatements = new ArrayList<>();
        for (String sql : sqlStatements) {
            String trimmedSql = sql.trim().toUpperCase();
            if (trimmedSql.startsWith("ADD JAR")) {
                System.out.println("Skipping ADD JAR statement from SQL file: " + 
                    sql.substring(0, Math.min(80, sql.length())) + "...");
            } else {
                filteredStatements.add(sql);
            }
        }
        
        // Check if any SQL contains Python UDF
        boolean usesPythonUDF = filteredStatements.stream()
            .anyMatch(sql -> sql.toUpperCase().contains("LANGUAGE PYTHON"));
        
        org.apache.flink.table.api.TableEnvironment tableEnv;
        
        if (usesPythonUDF) {
            System.out.println("Python UDF detected - using TableEnvironment with Python support");
            
            // Use TableEnvironment for Python UDF support
            org.apache.flink.table.api.EnvironmentSettings settings = 
                org.apache.flink.table.api.EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();
            tableEnv = org.apache.flink.table.api.TableEnvironment.create(settings);
            
            // Configure Python environment
            configurePythonEnvironment(tableEnv);
        } else {
            // Use StreamTableEnvironment for regular SQL
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            tableEnv = StreamTableEnvironment.create(env);
        }
        
        // Load Java JARs from parameter (comma-separated)
        if (javaJars != null && !javaJars.isEmpty()) {
            String[] jarPaths = javaJars.split(",");
            for (String jarPath : jarPaths) {
                jarPath = jarPath.trim();
                if (!jarPath.isEmpty()) {
                    String absolutePath = new java.io.File(jarPath).getAbsolutePath();
                    String addJarSql = "ADD JAR '" + absolutePath + "'";
                    System.out.println("Loading Java JAR: " + absolutePath);
                    tableEnv.executeSql(addJarSql);
                    System.out.println("✓ JAR loaded successfully");
                }
            }
        }
        
        TableResult finalResult = null;
        for (String sql : filteredStatements) {
            // Parse and update WITH clause if present
            sql = updateWithClause(sql);
            
            System.out.println("Executing SQL: " + sql);
            
            finalResult = tableEnv.executeSql(sql);
            
            // For non-INSERT statements, wait for completion
            if (!sql.trim().toUpperCase().startsWith("INSERT")) {
                System.out.println("✓ Statement executed successfully");
            }
        }

        return finalResult;
    }
    
    /**
     * Configures Python environment for PyFlink UDF support
     */
    private void configurePythonEnvironment(org.apache.flink.table.api.TableEnvironment tableEnv) {
        // Get Configuration from TableEnvironment
        org.apache.flink.configuration.Configuration config = tableEnv.getConfig().getConfiguration();
        
        System.out.println("Configuring Python UDF support...");
        
        // Try to detect Python executable
        String pythonExecutable = detectPythonExecutable();
        if (pythonExecutable != null) {
            config.setString("python.executable", pythonExecutable);
            config.setString("python.client.executable", pythonExecutable);
            System.out.println("Using Python executable: " + pythonExecutable);
        } else {
            System.out.println("Using default Python executable (python3)");
            config.setString("python.executable", "python3");
            config.setString("python.client.executable", "python3");
        }
        
        // Set Python files path from parameter
        if (pythonPath != null && !pythonPath.isEmpty()) {
            java.io.File pythonDir = new java.io.File(pythonPath);
            
            if (pythonDir.isDirectory()) {
                // If it's a directory, find all .py files and add them
                java.io.File[] pyFiles = pythonDir.listFiles((dir, name) -> name.endsWith(".py"));
                
                if (pyFiles != null && pyFiles.length > 0) {
                    StringBuilder pythonFilesBuilder = new StringBuilder();
                    for (int i = 0; i < pyFiles.length; i++) {
                        if (i > 0) {
                            pythonFilesBuilder.append(",");
                        }
                        pythonFilesBuilder.append(pyFiles[i].getAbsolutePath());
                        System.out.println("Adding Python file: " + pyFiles[i].getAbsolutePath());
                    }
                    config.setString("python.files", pythonFilesBuilder.toString());
                } else {
                    System.out.println("Warning: No .py files found in " + pythonPath);
                }
            } else if (pythonDir.isFile() && pythonPath.endsWith(".py")) {
                // If it's a single .py file
                String absolutePath = pythonDir.getAbsolutePath();
                config.setString("python.files", absolutePath);
                System.out.println("Adding Python file: " + absolutePath);
            } else {
                System.out.println("Warning: Python path does not exist or is invalid: " + pythonPath);
            }
        } else {
            System.out.println("Warning: No Python path configured");
        }
        
        // Check if Python support is available
        try {
            Class.forName("org.apache.flink.python.PythonFunctionRunner");
            System.out.println("✓ Python support detected in classpath");
        } catch (ClassNotFoundException e) {
            System.err.println("WARNING: flink-python jar not found in classpath!");
        }
        
        System.out.println("✓ Python UDF configuration completed");
    }
    
    /**
     * Detects available Python executable
     */
    private String detectPythonExecutable() {
        // Try common Python paths
        String[] possiblePaths = {
            "/opt/flink/pyflink/.venv/bin/python",  // Docker path
            System.getProperty("user.home") + "/.venv/bin/python",  // Local venv
            "python3",  // System Python
            "python"
        };
        
        for (String path : possiblePaths) {
            if (isPythonAvailable(path)) {
                return path;
            }
        }
        
        return null;
    }
    
    /**
     * Checks if Python is available at given path
     */
    private boolean isPythonAvailable(String pythonPath) {
        try {
            Process process = Runtime.getRuntime().exec(new String[]{pythonPath, "--version"});
            int exitCode = process.waitFor();
            return exitCode == 0;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Updates the WITH clause in SQL statements by parsing properties into a Map,
     * modifying them, and reconstructing the WITH clause.
     */
    private String updateWithClause(String sql) {
        // Check if SQL contains WITH clause
        if (!sql.toUpperCase().contains("WITH")) {
            return sql;
        }
        
        // Extract WITH clause properties
        Map<String, String> properties = extractWithProperties(sql);
        
        if (properties.isEmpty()) {
            return sql;
        }
        
        // Update bootstrap.servers
        if (properties.containsKey("properties.bootstrap.servers")) {
            properties.put("properties.bootstrap.servers", bootstrapServers);
            System.out.println("Updated bootstrap.servers to: " + bootstrapServers);
        }
        
        // Remove SSL/security properties that interfere with testcontainers
        List<String> securityPropertiesToRemove = Arrays.asList(
            "properties.security.protocol",
            "properties.ssl.keystore.location",
            "properties.ssl.keystore.password",
            "properties.ssl.truststore.location",
            "properties.ssl.truststore.password",
            "properties.ssl.key.password",
            "properties.ssl.endpoint.identification.algorithm",
            "properties.sasl.mechanism",
            "properties.sasl.jaas.config"
        );
        
        for (String securityProp : securityPropertiesToRemove) {
            if (properties.remove(securityProp) != null) {
                System.out.println("Removed security property for testing: " + securityProp);
            }
        }
        
        // Reconstruct SQL with updated properties
        return reconstructSqlWithProperties(sql, properties);
    }
    
    /**
     * Extracts properties from WITH clause into a Map.
     * Example: 'connector' = 'kafka' -> Map.entry("connector", "kafka")
     */
    private Map<String, String> extractWithProperties(String sql) {
        Map<String, String> properties = new java.util.LinkedHashMap<>();
        
        // Find WITH clause
        int withIndex = sql.toUpperCase().indexOf("WITH");
        if (withIndex == -1) {
            return properties;
        }
        
        // Extract content between WITH ( and )
        int startParen = sql.indexOf("(", withIndex);
        int endParen = sql.lastIndexOf(")");
        
        if (startParen == -1 || endParen == -1) {
            return properties;
        }
        
        String withContent = sql.substring(startParen + 1, endParen);
        
        // Parse properties: 'key' = 'value'
        // This regex matches: 'key' = 'value' or "key" = "value"
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
            "['\"](.*?)['\"]\\s*=\\s*['\"](.*?)['\"]"
        );
        java.util.regex.Matcher matcher = pattern.matcher(withContent);
        
        while (matcher.find()) {
            String key = matcher.group(1);
            String value = matcher.group(2);
            properties.put(key, value);
        }
        
        return properties;
    }
    
    /**
     * Reconstructs SQL statement with updated properties in the WITH clause.
     */
    private String reconstructSqlWithProperties(String sql, Map<String, String> properties) {
        // Find WITH clause boundaries
        int withIndex = sql.toUpperCase().indexOf("WITH");
        if (withIndex == -1) {
            return sql;
        }
        
        int startParen = sql.indexOf("(", withIndex);
        int endParen = sql.lastIndexOf(")");
        
        if (startParen == -1 || endParen == -1) {
            return sql;
        }
        
        // Build new WITH clause
        StringBuilder withClause = new StringBuilder();
        int count = 0;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (count > 0) {
                withClause.append(",\n  ");
            }
            withClause.append("'").append(entry.getKey()).append("' = '")
                      .append(entry.getValue()).append("'");
            count++;
        }
        
        // Reconstruct SQL
        String before = sql.substring(0, startParen + 1);
        String after = sql.substring(endParen);
        
        return before + "\n  " + withClause.toString() + "\n" + after;
    }

    private List<String> loadSQLFromFile(String filePath) throws IOException {
        System.out.println("Loading SQL from: " + filePath);
        
        String content = new String(Files.readAllBytes(Paths.get(filePath)));
        
        // Split by delimiter (-----)
        String[] statements = content.split("-----");
        
        List<String> sqlStatements = new ArrayList<>();
        for (String stmt : statements) {
            // Remove comment lines and trim
            String cleaned = removeComments(stmt);
            if (!cleaned.isEmpty()) {
                sqlStatements.add(cleaned);
            }
        }
        
        System.out.println("Loaded " + sqlStatements.size() + " SQL statements");
        return sqlStatements;
    }
    
    private String removeComments(String sql) {
        StringBuilder result = new StringBuilder();
        String[] lines = sql.split("\n");
        
        for (String line : lines) {
            String trimmedLine = line.trim();
            // Skip empty lines and comment-only lines
            if (!trimmedLine.isEmpty() && !trimmedLine.startsWith("--")) {
                result.append(line).append("\n");
            }
        }
        
        return result.toString().trim();
    }

    private void verifyOutput() throws Exception {
        // Load expected messages with topics from file
        List<TopicMessage> expectedMessages = loadMessagesWithTopicsFromFile(outputFile);
        
        // Group expected messages by topic
        Map<String, List<String>> expectedByTopic = new HashMap<>();
        for (TopicMessage tm : expectedMessages) {
            expectedByTopic.computeIfAbsent(tm.topic, k -> new ArrayList<>()).add(tm.message);
        }
        
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-file");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Subscribe to all output topics
        Set<String> outputTopics = expectedByTopic.keySet();
        System.out.println("Consuming from topics: " + outputTopics);
        
        Map<String, List<String>> actualByTopic = new HashMap<>();
        
        int totalExpected = expectedMessages.size();
        int totalReceived = 0;
            
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(outputTopics);
            
            long endTime = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < endTime && totalReceived < totalExpected) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received from " + record.topic() + ": " + record.value());
                    actualByTopic.computeIfAbsent(record.topic(), k -> new ArrayList<>())
                                 .add(record.value());
                    totalReceived++;
                }
            }
        }
        
        // Verify results
        System.out.println("Total messages received: " + totalReceived);
        assertEquals(expectedMessages.size(), totalReceived, 
            "Expected " + expectedMessages.size() + " messages");
        
        // Verify each topic's messages
        for (Map.Entry<String, List<String>> entry : expectedByTopic.entrySet()) {
            String topic = entry.getKey();
            Set<String> expectedSet = new HashSet<>(entry.getValue());
            Set<String> actualSet = new HashSet<>(actualByTopic.getOrDefault(topic, new ArrayList<>()));
            
            for (String expected : expectedSet) {
                assertTrue(actualSet.contains(expected), 
                    "Expected message not found in topic " + topic + ": " + expected + 
                    "\nActual messages: " + actualByTopic.get(topic));
            }
        }
        
        System.out.println("✓ Test passed! Output matches expected results.");
    }
}