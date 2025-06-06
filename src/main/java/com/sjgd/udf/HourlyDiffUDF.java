package com.sjgd.udf;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HourlyDiffUDF implements UDTF {
    private List<DataPoint> dataPoints = new ArrayList<>();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private boolean debug = false;
    
    private void log(String message) {
        System.out.println("[HourlyDiffUDF] " + message);
    }
    
    private void debugLog(String message) {
        if (debug) {
            System.out.println("[HourlyDiffUDF Debug] " + message);
        }
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
        // 从参数中获取debug开关
        debug = parameters.getBooleanOrDefault("debug", false);
        log("=== UDF Started ===");
        debugLog("Parameters: " + parameters);
    }

    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        if (!row.isNull(0)) {
            long timestamp = row.getTime();
            double value = row.getDouble(0);
            dataPoints.add(new DataPoint(timestamp, value));
            debugLog("=== New Data Point ===");
            debugLog("Raw timestamp: " + timestamp);
            debugLog("Time: " + LocalDateTime.ofEpochSecond(timestamp/1000, (int)((timestamp % 1000) * 1000000), ZoneOffset.UTC).format(formatter));
            debugLog("Value: " + value);
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
        debugLog("=== All Data Points ===");
        for (int i = 0; i < dataPoints.size(); i++) {
            DataPoint dp = dataPoints.get(i);
            debugLog("Point " + i + " - Raw timestamp: " + dp.timestamp);
            debugLog("Point " + i + " - Time: " + 
                LocalDateTime.ofEpochSecond(dp.timestamp/1000, (int)((dp.timestamp % 1000) * 1000000), ZoneOffset.UTC).format(formatter) +
                ", Value: " + dp.value);
        }
        
        // 记录数据点范围
        debugLog("=== Data Points Summary ===");
        debugLog("Total points: " + dataPoints.size());
        debugLog("Time range: " + 
            LocalDateTime.ofEpochSecond(dataPoints.get(0).timestamp/1000, (int)((dataPoints.get(0).timestamp % 1000) * 1000000), ZoneOffset.UTC).format(formatter) +
            " to " +
            LocalDateTime.ofEpochSecond(dataPoints.get(dataPoints.size()-1).timestamp/1000, (int)((dataPoints.get(dataPoints.size()-1).timestamp % 1000) * 1000000), ZoneOffset.UTC).format(formatter));
        
        // 获取当前时间（使用UTC时区）
        LocalDateTime rawTime = LocalDateTime.now(ZoneOffset.UTC);
        debugLog("=== Raw Current Time ===");
        debugLog("Raw time: " + rawTime.format(formatter));
        
        // 计算最近的整点时间（考虑30秒容差）
        LocalDateTime currentTime;
        if (rawTime.getMinute() == 59 && rawTime.getSecond() >= 30) {
            // 如果当前时间在59分30秒之后，认为是下一个整点
            currentTime = rawTime.plusHours(1)
                .withMinute(0)
                .withSecond(0)
                .withNano(0);
        } else {
            // 否则认为是当前整点
            currentTime = rawTime
                .withMinute(0)
                .withSecond(0)
                .withNano(0);
        }
        
        // 计算目标整点时间
        LocalDateTime tenOClock = currentTime.minusHours(1);  // 前一个整点
        LocalDateTime nineOClock = tenOClock.minusHours(1);   // 再前一个整点
        
        debugLog("=== Target Time Points ===");
        debugLog("Current time: " + currentTime.format(formatter));
        debugLog("Ten o'clock: " + tenOClock.format(formatter));
        debugLog("Nine o'clock: " + nineOClock.format(formatter));
        
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
            debugLog("=== Not Enough Points for Interpolation ===");
            debugLog("Required: 2, Got: " + dataPoints.size());
            return null;
        }
        
        debugLog("=== Starting Interpolation ===");
        debugLog("Target time: " + LocalDateTime.ofEpochSecond(targetTime/1000, (int)((targetTime % 1000) * 1000000), ZoneOffset.UTC).format(formatter));
        debugLog("Using " + dataPoints.size() + " data points");
        
        try {
            // 按时间戳排序
            dataPoints.sort(Comparator.comparing(dp -> dp.timestamp));
            
            // 准备数据点
            double[] x = new double[dataPoints.size()];
            double[] y = new double[dataPoints.size()];
            
            for (int i = 0; i < dataPoints.size(); i++) {
                x[i] = dataPoints.get(i).timestamp;
                y[i] = dataPoints.get(i).value;
            }
            
            // 使用样条插值
            SplineInterpolator interpolator = new SplineInterpolator();
            PolynomialSplineFunction spline = interpolator.interpolate(x, y);
            
            // 计算插值结果
            double result = spline.value(targetTime);
            
            debugLog("=== Interpolation Complete ===");
            debugLog("Result: " + result);
            return result;
        } catch (Exception e) {
            debugLog("=== Interpolation Failed ===");
            debugLog("Error: " + e.getMessage());
            return null;
        }
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