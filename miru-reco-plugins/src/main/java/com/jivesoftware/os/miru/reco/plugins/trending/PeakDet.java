package com.jivesoftware.os.miru.reco.plugins.trending;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class PeakDet {

    public static void main(String[] args) {
        long[] vector = new long[] {
            0, 1, 2, 3, 4, 5, 1, 2, 3, 4, 8, 7, 6, 5, 4, 3, 2, 1
        };
        List<Peak> peaks = new PeakDet().peakdet(vector, 1);

        for (Peak peak : peaks) {
            System.out.println(peak);
        }
    }

    List<Peak> peakdet(long[] vector, double triggerDelta) {
        return peakdet(vector, 0, vector.length, triggerDelta);
    }

    /*
     !PEAKDET Detect peaks in a vector
     !
     !        call PEAKDET(MAXTAB, MINTAB, N, V, DELTA) finds the local
     !        maxima and minima ("peaks") in the vector V of size N.
     !        MAXTAB and MINTAB consists of two columns. Column 1
     !        contains indices in V, and column 2 the found values.
     !
     !        call PEAKDET(MAXTAB, MINTAB, N, V, DELTA, X) replaces the
     !        indices in MAXTAB and MINTAB with the corresponding X-values.
     !
     !        A point is considered a maximum peak if it has the maximal
     !        value, and was preceded (to the left) by a value lower by
     !        DELTA.
     !
     ! Eli Billauer, 3.4.05 (http://billauer.co.il)
     ! Translated into Fortran by Brian McNoldy (http://andrew.rsmas.miami.edu/bmcnoldy)
     ! This function is released to the public domain; Any use is allowed.*/
    List<Peak> peakdet(long[] vector, int offset, int length, double triggerDelta) {
        double mn = Double.POSITIVE_INFINITY;
        double mx = Double.NEGATIVE_INFINITY;
        double mnpos = Double.NaN;
        double mxpos = Double.NaN;
        int lookformax = 1;

        List<Peak> maxtab_tmp = new ArrayList<>();
        //List<Valley> mintab_tmp = new ArrayList<>();

        for (int i = offset; i < length; i++) {
            double a = vector[i];
            if (a > mx) {
                mx = a;
                mxpos = vector[i];
            }
            if (a < mn) {
                mn = a;
                mnpos = vector[i];
            }
            if (lookformax == 1) {
                if (a < mx - triggerDelta) {
                    maxtab_tmp.add(new Peak(mxpos, i));
                    mn = a;
                    mnpos = vector[i];
                    lookformax = 0;
                }
            } else {
                if (a > mn + triggerDelta) {
                    //mintab_tmp.add(new Valley(mnpos, i));
                    mx = a;
                    mxpos = vector[i];
                    lookformax = 1;
                }
            }
        }

        return maxtab_tmp;
    }

    static class Peak {

        public final double height;
        public final int index;

        private Peak(double height, int index) {
            this.height = height;
            this.index = index;
        }

        @Override
        public String toString() {
            return "Peak{" + "height=" + height + ", index=" + index + '}';
        }

    }

    static class Valley {

        public final double height;
        public final int index;

        private Valley(double height, int index) {
            this.height = height;
            this.index = index;
        }

        @Override
        public String toString() {
            return "Valley{" + "height=" + height + ", index=" + index + '}';
        }

    }
}
