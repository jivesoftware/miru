package com.googlecode.javaewah.symmetric;

/**
 * Generic interface to compute symmetric Boolean functions.
 * 
 * @see <a
 *      href="http://en.wikipedia.org/wiki/Symmetric_Boolean_function">http://en.wikipedia.org/wiki/Symmetric_Boolean_function</a>
 * @author Daniel Lemire
 * @since 0.8.0
 **/
public interface BitmapSymmetricAlgorithm {
        /**
         * Compute a Boolean symmetric query.
         * 
         * @param f
         *                symmetric boolean function to be processed
         * @param out
         *                the result of the query
         * @param set
         *                the inputs
         */
        public void symmetric(com.googlecode.javaewah.symmetric.UpdateableBitmapFunction f, com.googlecode.javaewah.BitmapStorage out,
                              com.googlecode.javaewah.EWAHCompressedBitmap... set);
}
