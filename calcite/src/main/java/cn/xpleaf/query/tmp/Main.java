package cn.xpleaf.query.tmp;

import cn.xpleaf.query.dataset.Dataset;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        String datasetJson = "{\n" +
                "  \"modelVersion\": 1,\n" +
                "  \"name\": \"teachers\",\n" +
                "  \"schema\":[\n" +
                "    {\"name\": \"name\", \"columnType\": \"string\"},\n" +
                "    {\"name\": \"age\", \"columnType\": \"long\"},\n" +
                "    {\"name\": \"rate\", \"columnType\": \"double\"},\n" +
                "    {\"name\": \"percent\", \"columnType\": \"float\"},\n" +
                "    {\"name\": \"join_time\", \"columnType\": \"long\", \"properties\":{\"type\":\"timestamp\"}}\n" +
                "  ],\n" +
                "  \"storage\": {\n" +
                "  \t\"type\": \"elasticsearch\",\n" +
                "    \"datasource\": \"teachers\"\n" +
                "  }\n" +
                "}";

        Dataset dataset = Dataset.constructDataset(datasetJson);

        System.out.println(dataset);

    }



}
