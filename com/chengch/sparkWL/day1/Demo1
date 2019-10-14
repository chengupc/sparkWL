package com.chengch.sparkWL.day1;

import java.util.ArrayList;

import com.chengch.sparkWL.day1.Utills.MyUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.javatuples.Triplet;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.chengch.sparkWL.day1.Utills.MyUtil.*;

public class Test {
    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("countIp").setMaster("local[2]");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> ipRulesRDD = jsc.textFile(args[0],1);

        JavaRDD<Triplet> ipLines = ipRulesRDD.map(new Function<String, Triplet>() {
            List<Triplet> tmp = new ArrayList<>();

            @Override
            public Triplet call(String line) throws Exception {

                Triplet<Long, Long, String> triplet = null;
                String[] split = line.split("\\|");
                Long startNum = Long.valueOf(split[2]);
                Long endNum = Long.valueOf(split[3]);
                String province = split[6];
                triplet = Triplet.with(startNum, endNum, province);
                return triplet;
            }
        });
        List<Triplet> ipLinesList = ipLines.collect();
        Broadcast<List<Triplet>> broadcast = jsc.broadcast(ipLinesList);

        JavaRDD<String> accessRDD = jsc.textFile(args[1],1);
        JavaRDD<String> province = accessRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] lineSplit = line.split("\\|");
                String ipStr = lineSplit[1];
                Long ipLong = ip2Num(ipStr);
                List<Triplet> value = broadcast.value();
                int index = MyUtil.getIndex(value, ipLong);
                String province = "未知";
                if (index != -1) {
                    province = (String) value.get(index).getValue2();
                }
                return Arrays.asList(province).iterator();
            }
        });

        //provinceAndOne
        JavaPairRDD<String, Integer> provinceAndOne = province.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        //reduce
        JavaPairRDD<String, Integer> reduced = provinceAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //逆序
        JavaPairRDD<Integer, String> change1= reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        //排序
        JavaPairRDD<Integer, String> sorted = change1.sortByKey(false);

        //排序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });

        //保存
        System.out.println(result.collect().toString());
        System.out.println(result.partitions().size());
        result.foreachPartition(it->data2Mysql(it));

    }


}
