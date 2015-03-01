package com.yahoo.ycsb.measurements;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Measurement class for operations with multiple steps like scans.
 *
 * Created by eshcar on 22/10/14.
 */
public class MultipleStepsMeasurement {

    private String _stepName;
    private int _steps;
    private AtomicIntegerArray _latencyAgg;
    private AtomicIntegerArray _opAgg;
    private AtomicInteger _negativeCounter;

    public static int numops=0;
    private double _timeout = 0;


    public MultipleStepsMeasurement(String name, int numSteps) {
        this._stepName = name;
        this._steps = numSteps;
        this._latencyAgg = new AtomicIntegerArray(numSteps);
        this._opAgg = new AtomicIntegerArray(numSteps);
        this._negativeCounter = new AtomicInteger(0);
    }

    public String getName() {
        return _stepName;
    }

    public void measure(int latency, int step) {
        if(latency < 0) {
            _negativeCounter.getAndIncrement();
        } else {
            _latencyAgg.getAndAdd(step,latency);
            _opAgg.getAndIncrement(step);
        }
    }

    public void measureTimeout(double timeout) {
        _timeout += timeout;
        numops++;
    }

    public void exportMeasurements(MeasurementsExporter exporter) throws IOException {

        exporter.write(getName(),"Sleep per next operations(us)",(_timeout /numops));

        if (_negativeCounter.intValue() > 0) {
            exporter.write(getName(),"Negative Latency", _negativeCounter.intValue());
        }

        for (int i = 0; i < _steps; i++) {
            exporter.write(getName(),"step:"+i+", operations: "+_opAgg.get(i)+", Average Latency(us)",(int)((double)_latencyAgg.get(i)/(double)_opAgg.get(i)));
        }

    }


}
