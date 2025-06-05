package com.sjgd.udf;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HourlyDiffUDF implements UDTF {
    private List<DataPoint> dataPoints = new ArrayList<>();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
    private void log(String message) {
        System.out.println("[HourlyDiffUDF] " + message);
    }
    
    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        validator.validateInputSeriesNumber(1)
                 .validateInputSeriesDataType(0, Type.DOUBLE);
    }

    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception {
        configurations.setAccessStrategy(new RowByRowAccessStrategy())
                     .setOutputDataType(Type.DOUBLE);
        log("=== UDF Started ===");
        log("Parameters: " + parameters);
    }

    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        if (!row.isNull(0)) {
            long timestamp = row.getTime();
            double value = row.getDouble(0);
            dataPoints.add(new DataPoint(timestamp, value));
            log("=== New Data Point ===");
            log("Time: " + LocalDateTime.ofEpochSecond(timestamp/1000, 0, ZoneOffset.UTC).format(formatter));
            log("Value: " + value);
        }
    }

    @Override
    public void terminate(PointCollector collector) throws Exception {
        if (dataPoints.isEmpty()) {
            log("=== No Data Points Received ===");
            return;
        }
        
        // 按时间排序
        dataPoints.sort(Comparator.comparing(dp -> dp.timestamp));
        
        // 记录所有数据点
        log("=== All Data Points ===");
        for (int i = 0; i < dataPoints.size(); i++) {
            DataPoint dp = dataPoints.get(i);
            log("Point " + i + " - Time: " + 
                LocalDateTime.ofEpochSecond(dp.timestamp/1000, 0, ZoneOffset.UTC).format(formatter) +
                ", Value: " + dp.value);
        }
        
        // 记录数据点范围
        log("=== Data Points Summary ===");
        log("Total points: " + dataPoints.size());
        log("Time range: " + 
            LocalDateTime.ofEpochSecond(dataPoints.get(0).timestamp/1000, 0, ZoneOffset.UTC).format(formatter) +
            " to " +
            LocalDateTime.ofEpochSecond(dataPoints.get(dataPoints.size()-1).timestamp/1000, 0, ZoneOffset.UTC).format(formatter));
        
        // 获取当前时间并取整点
        LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC)
            .withMinute(0)
            .withSecond(0)
            .withNano(0);
        
        // 计算目标整点时间
        LocalDateTime tenOClock = currentTime.minusHours(1);  // 前一个整点
        LocalDateTime nineOClock = tenOClock.minusHours(1);   // 再前一个整点
        
        log("=== Target Time Points ===");
        log("Current time: " + currentTime.format(formatter));
        log("Ten o'clock: " + tenOClock.format(formatter));
        log("Nine o'clock: " + nineOClock.format(formatter));
        
        // 插值计算整点值
        Double nineValue = interpolateValue(nineOClock.toInstant(ZoneOffset.UTC).toEpochMilli());
        Double tenValue = interpolateValue(tenOClock.toInstant(ZoneOffset.UTC).toEpochMilli());
        
        if (nineValue != null && tenValue != null) {
            double diff = tenValue - nineValue;
            log("=== Final Results ===");
            log("Nine o'clock value: " + nineValue);
            log("Ten o'clock value: " + tenValue);
            log("Difference: " + diff);
            collector.putDouble(tenOClock.toInstant(ZoneOffset.UTC).toEpochMilli(), diff);
        } else {
            log("=== Interpolation Failed ===");
            log("Nine o'clock value: " + nineValue);
            log("Ten o'clock value: " + tenValue);
        }
    }

    private Double interpolateValue(long targetTime) {
        if (dataPoints.size() < 2) {
            log("=== Not Enough Points for Interpolation ===");
            log("Required: 2, Got: " + dataPoints.size());
            return null;
        }
        
        log("=== Starting Interpolation ===");
        log("Target time: " + LocalDateTime.ofEpochSecond(targetTime/1000, 0, ZoneOffset.UTC).format(formatter));
        log("Using " + dataPoints.size() + " data points");
            
        double result = lagrangeInterpolate(dataPoints, targetTime);
        log("=== Interpolation Complete ===");
        log("Result: " + result);
        return result;
    }

    private double lagrangeInterpolate(List<DataPoint> points, long targetTime) {
        double result = 0.0;
        log("=== Lagrange Interpolation Details ===");
        log("Target time: " + LocalDateTime.ofEpochSecond(targetTime/1000, 0, ZoneOffset.UTC).format(formatter));
        
        for (int i = 0; i < points.size(); i++) {
            double term = points.get(i).value;
            log("--- Term " + i + " ---");
            log("Base value: " + term);
            log("Time: " + LocalDateTime.ofEpochSecond(points.get(i).timestamp/1000, 0, ZoneOffset.UTC).format(formatter));
                
            for (int j = 0; j < points.size(); j++) {
                if (i != j) {
                    double factor = (targetTime - points.get(j).timestamp) / 
                                  (points.get(i).timestamp - points.get(j).timestamp);
                    term *= factor;
                    log("Factor for point " + j + ": " + factor);
                }
            }
            result += term;
            log("Term " + i + " result: " + term);
        }
        
        log("=== Lagrange Final Result ===");
        log("Result: " + result);
        return result;
    }

    private static class DataPoint {
        long timestamp;
        double value;

        DataPoint(long timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }
    }
}