package com.jivesoftware.os.miru.manage.deployable.analytics;

/**
 *
 * @author jonathan
 */
public class MinMaxDouble {

    /**
     *
     */
    public double min = Double.MAX_VALUE;
    /**
     *
     */
    public double max = -Double.MAX_VALUE;
    /**
     *
     */
    public int minIndex = -1;
    /**
     *
     */
    public int maxIndex = -1;
    private float sum = 0;
    private int count = 0;

    /**
     *
     */
    public MinMaxDouble() {
    }

    /**
     *
     * @param _min
     * @param _max
     */
    public MinMaxDouble(double _min, double _max) {
        min = _min;
        max = _max;
        count = 2;
    }

    /**
     *
     * @param _value
     * @return
     */
    public double std(double _value) {
        double mean = Math.pow(mean(), 2);
        double value = Math.pow((double) _value, 2);
        double variation = Math.max(mean, value) - Math.min(mean, value);
        return Math.sqrt(variation);
    }

    /**
     *
     * @param _p
     * @return
     */
    public boolean inclusivelyContained(double _p) {
        if (_p < min) {
            return false;
        }
        if (_p > max) {
            return false;
        }
        return true;
    }

    /**
     *
     * @return
     */
    public double min() {
        return min;
    }

    /**
     *
     * @return
     */
    public double max() {
        return max;
    }

    /**
     *
     * @param _double
     * @return
     */
    public double value(double _double) {
        sum += _double;
        if (_double > max) {
            max = _double;
            maxIndex = count;
        }
        if (_double < min) {
            min = _double;
            minIndex = count;
        }
        count++;
        return _double;
    }

    /**
     *
     */
    public void reset() {
        min = Double.MAX_VALUE;
        max = -Double.MAX_VALUE;
        minIndex = -1;
        maxIndex = -1;
        sum = 0;
        count = 0;
    }

    /**
     *
     * @return
     */
    public long samples() {
        return count;
    }

    /**
     *
     * @return
     */
    public double mean() {
        return sum / (double) count;
    }

    /**
     *
     * @return
     */
    public double range() {
        return max - min;
    }

    /**
     *
     * @return
     */
    public double middle() {
        return min + ((max - min) / 2);
    }

    /**
     *
     * @param _v
     * @param _inclusive
     * @return
     */
    public boolean isBetween(double _v, boolean _inclusive) {
        if (_inclusive) {
            return _v <= max && _v >= min;
        } else {
            return _v < max && _v > min;
        }
    }

    /**
     *
     * @param _double
     * @return
     */
    public double negativeOneToOne(double _double) {
        return (zeroToOne(_double) - 0.5) * 2.0;
    }

    /**
     *
     * @param _double
     * @return
     */
    public double zeroToOne(double _double) {
        return zeroToOne(min, max, _double);
    }

    /**
     *
     * @param _min
     * @param _max
     * @param _double
     * @return
     */
    public static final double zeroToOne(double _min, double _max, double _double) {
        return (_double - _min) / (_max - _min);
    }

    /**
     *
     * @param _double
     * @return
     */
    public double unzeroToOne(double _double) {
        return unzeroToOne(min, max, _double);
    }

    /**
     *
     * @param _min
     * @param _max
     * @param _double
     * @return
     */
    public static final double unzeroToOne(double _min, double _max, double _double) {
        return ((_max - _min) * _double) + _min;
    }

    @Override
    public String toString() {
        return "Min:" + min + " Max:" + max;
    }

}
