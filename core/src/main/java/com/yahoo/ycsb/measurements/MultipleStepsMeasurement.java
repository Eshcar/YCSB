package com.yahoo.ycsb.measurements;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Measurement class for operations with multiple steps like scans.
 * Created by eshcar on 22/10/14.
 */
public class MultipleStepsMeasurement {

  private String _stepName;
  // maximum number of steps in the operation
  private int _steps;

  // Atomic fields to support multi-threading
  private AtomicIntegerArray _latencyAgg;
  private AtomicIntegerArray _stepAgg;
  private AtomicInteger _negativeCounter;
  private AtomicInteger _timeout = new AtomicInteger(0);

  public static AtomicInteger opsCounter = new AtomicInteger(0);

  public MultipleStepsMeasurement(String name, int numSteps) {
    this._stepName = name;
    this._steps = numSteps;
    this._latencyAgg = new AtomicIntegerArray(numSteps);
    this._stepAgg = new AtomicIntegerArray(numSteps);
    this._negativeCounter = new AtomicInteger(0);
    opsCounter = new AtomicInteger(0);
  }

  public String getName() {
    return _stepName;
  }

  public void measure(int latency, int step) {
    if (latency < 0) {
      _negativeCounter.getAndIncrement();
    } else {
      _latencyAgg.getAndAdd(step, latency);
      _stepAgg.getAndIncrement(step);
    }
  }

  public void measureTimeout(int timeout) {
    _timeout.addAndGet(timeout);
    opsCounter.getAndIncrement();
  }

  public void exportMeasurements(MeasurementsExporter exporter) throws IOException {

    exporter.write(getName(), "Sleep per next operations(us)",
        ((double)(_timeout.get()) / opsCounter.get()));

    if (_negativeCounter.intValue() > 0) {
      exporter.write(getName(), "Negative Latency", _negativeCounter.intValue());
    }

    for (int i = 0; i < _steps; i++) {
      exporter.write(getName(),
          "step:" + i + ", operations: " + _stepAgg.get(i) + ", Average Latency(us)",
          (int) ((double) _latencyAgg.get(i) / (double) _stepAgg.get(i)));
    }

  }

}
