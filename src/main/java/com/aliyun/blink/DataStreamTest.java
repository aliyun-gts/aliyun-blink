package com.aliyun.blink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class DataStreamTest {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        /* 如何在Blink Job中使用自动以的参数，只有提交作业时有效，在本地调试时要去掉*/
        /*
         * 在作业开发页面配置参数
         *
         * blink.main.class=<完整主类名>
         * --函数完整类名，例如com.alibaba.realtimecompute.DemoTableAPI。
         * blink.job.name=<作业名>
         * --例如datastream_test。
         * blink.main.jar=<完整主类名JAR包的资源名称>
         * --完整主类名JAR包的资源名称，例如blink_datastream.jar。
         */
        String jobName;
        /* 1. 此处必须写configFile，读取configFile中参数。*/
        String configFilePath = params.get("configFile");

        /* 2. 创建一个Properties对象用于保存在平台中设置的相关参数值。*/
        Properties properties = new Properties();

        /* 3. 将平台页面中设置的参数值加载到Properties对象中。*/
        properties.load(new StringReader(new String(Files.readAllBytes(Paths.get(configFilePath)), StandardCharsets.UTF_8)));

        /* 4. 获取参数。*/
        jobName = (String) properties.get("blink.job.name");
        System.out.println("Job name: " + jobName);

        // 创建Execution Environment。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过从本地TXT文件读取数据。
        //DataStream<String> text = env.readTextFile("./test.txt");
        DataStream<String> text = env.fromElements("hello world", "hello jason", "wonderful world");
        // 解析数据，先按word进行分组，然后进行开窗和聚合操作。
        DataStream<Tuple2<String, Integer>> windowCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // 将结果打印到控制台。请使用单线程打印，而非多线程打印。
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }
}
