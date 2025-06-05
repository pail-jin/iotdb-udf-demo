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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HourlyDiffUDF implements UDTF {
    private List<DataPoint> dataPoints = new ArrayList<>();
    
    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        validator.validateInputSeriesNumber(1)
                 .validateInputSeriesDataType(0, Type.DOUBLE);
    }

    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception {
        configurations.setAccessStrategy(new RowByRowAccessStrategy())
                     .setOutputDataType(Type.DOUBLE);
    }

    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        if (!row.isNull(0)) {
            long timestamp = row.getTime();
            double value = row.getDouble(0);
            dataPoints.add(new DataPoint(timestamp, value));
        }
    }

    @Override
    public void terminate(PointCollector collector) throws Exception {
        if (dataPoints.isEmpty()) {
            return;
        }
        
        // 按时间排序
        dataPoints.sort(Comparator.comparing(dp -> dp.timestamp));
        
        // 获取当前时间并取整点
        LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC)
            .withMinute(0)
            .withSecond(0)
            .withNano(0);
        
        // 计算目标整点时间
        LocalDateTime tenOClock = currentTime.minusHours(1);  // 前一个整点
        LocalDateTime nineOClock = tenOClock.minusHours(1);   // 再前一个整点
        
        // 插值计算整点值
        Double nineValue = interpolateValue(nineOClock.toInstant(ZoneOffset.UTC).toEpochMilli());
        Double tenValue = interpolateValue(tenOClock.toInstant(ZoneOffset.UTC).toEpochMilli());
        
        if (nineValue != null && tenValue != null) {
            double diff = tenValue - nineValue;
            collector.putDouble(tenOClock.toInstant(ZoneOffset.UTC).toEpochMilli(), diff);
        }
    }

    private Double interpolateValue(long targetTime) {
        if (dataPoints.size() < 2) {
            return null;
        }
        return lagrangeInterpolate(dataPoints, targetTime);
    }

    private double lagrangeInterpolate(List<DataPoint> points, long targetTime) {
        double result = 0.0;
        for (int i = 0; i < points.size(); i++) {
            double term = points.get(i).value;
            for (int j = 0; j < points.size(); j++) {
                if (i != j) {
                    term *= (targetTime - points.get(j).timestamp) / 
                           (points.get(i).timestamp - points.get(j).timestamp);
                }
            }
            result += term;
        }
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