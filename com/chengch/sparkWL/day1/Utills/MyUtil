package com.chengch.sparkWL.day1.Utills;
import java.sql.*;


import org.javatuples.Triplet;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MyUtil {

    public static List<Triplet> getRules(String path) {

        List<Triplet> rulesResult = new ArrayList<Triplet> ();
        try {
            BufferedReader reader = null;
            reader = new BufferedReader(new FileReader(path));
            String str;

            while((str = reader.readLine()) != null){

                Triplet<Long, Long, String> triplet =null;
                String[] split = str.split("\\|");
                Long startNum = Long.valueOf(split[2]);
                Long endNum = Long.valueOf(split[3]);
                String province = split[6];
                triplet = Triplet.with(startNum,endNum,province);
                rulesResult.add(triplet);
            }
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        return rulesResult;
    }
    public static List<String> getAccess(String path){
        List<String> ipStr = new ArrayList<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(path));
            String str;
            while((str = reader.readLine()) != null){
                String[] split = str.split("\\|");
                ipStr.add(split [1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ipStr;
    }

    public static Long ip2Num(String ip){
        String[] split = ip.split("\\.");
        List<String> arr = Arrays.asList(split);
        Long ipNum = 0L;
        for(int i=0; i< arr.size(); i++){
            ipNum = Long.valueOf(arr.get(i)) | ipNum << 8L;
        }
        return ipNum;
    };

    public static int getIndex(List<Triplet> ipLines, Long ip){
        int low = 0;
        int high = ipLines.size() -1;
        int middle = 0;

        while(low <= high){
            middle = (low + high) / 2;
            if(ip >= ((Long)ipLines.get(middle).getValue0()) && (ip <= ((Long) ipLines.get(middle).getValue1()))){
                return middle;

            }
            if(ip < ((Long)ipLines.get(middle).getValue0())){
                high = middle -1 ;
            }
            else{
                low = middle +1 ;
            }
        }
        return -1;
    }
    public static void data2Mysql(Iterator<Tuple2<String, Integer>> it) {
        Connection conn = null;
        conn = getConnection(conn);

        PreparedStatement ps = null;

        String sql = "insert into ipCountResult (province, number) values(?,?)";
        while (it.hasNext()){
            Tuple2<String, Integer> pair = it.next();
            try {
                ps = conn.prepareStatement(sql);
                ps.setString(1,pair._1);
                ps.setInt(2,pair._2);
                ps.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void data2MysqlTest(Iterator<Tuple2<String, Integer>> it) {
        while (it.hasNext()){
            Tuple2<String, Integer> tuple = it.next();
            System.out.println(tuple._1);
            System.out.println(tuple._2);
        }
    }

    public static Connection getConnection(Connection conn){
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.119.132:3306/sparkWL?characterEncoding=utf8","root", "199214");
            System.out.println("get sucess");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("failed");
        }
        return conn;
    }
    public static ResultSet getStatement(Connection conn, Statement stmt, ResultSet rs, String sql){
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;


    }
    public static void getInsert(Connection conn, IpInfo ipInfo, PreparedStatement ps){
        String sql = "insert into ipCountResult (province, number) values(?,?)";
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1,ipInfo.getProvince());
            ps.setInt(2,ipInfo.getCounter());
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}

