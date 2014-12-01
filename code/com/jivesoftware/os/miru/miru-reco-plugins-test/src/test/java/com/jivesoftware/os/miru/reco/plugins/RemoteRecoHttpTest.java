package com.jivesoftware.os.miru.reco.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoAnswer;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoConstants;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.OldTrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.Charsets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 *
 */
public class RemoteRecoHttpTest {

    private static final String[] REMOTE_HOSTS = new String[] {
        "soa-prime-data6.phx1.jivehosted.com",
        "soa-prime-data7.phx1.jivehosted.com",
        "soa-prime-data8.phx1.jivehosted.com",
        "soa-prime-data9.phx1.jivehosted.com"
    };
    private static final int REMOTE_PORT = 10_004;

    @Test(enabled = false, description = "Needs REMOTE constants")
    public void testSystemTrending() throws Exception {

        /*
        final String[] tenants = new String[] {
            "EVy", "Nv9", "KJt", "WVB", "ZlR", "iXM", "lPm", "Z49", "nyc", "oFd", "yso", "1MO", "40M", "2RV", "999", "EVy", "Nv9", "KJt", "WVB", "ZlR",
            "iXM", "lPm", "Z49", "nyc", "yso", "1MO", "9yG", "999", "EVy", "Nv9", "KJt"
        };
        */
        final String[] tenants = new String[] { "000", "02o", "04j", "069", "08Y", "09q", "09u", "0AO", "0Hd", "0J1", "0Og", "0Pg", "0SN", "0Sp", "0Us",
            "0Yd", "0Zw", "0aP", "0c0", "0dk", "0hd", "0hg", "0hw", "0i1", "0pq", "0r5", "0r9", "0to", "0ww", "0xA", "0z4", "0zv", "10T", "125", "126",
            "175", "1Bm", "1Dp", "1Jv", "1MO", "1PL", "1To", "1Tq", "1Vx", "1Wd", "1Zf", "1bV", "1eK", "1f8", "1h7", "1hM", "1he", "1lU", "1lX",
            "1nF", "1pJ", "1rJ", "1v2", "1zu", "21v", "22s", "24l", "264", "2B8", "2BR", "2D4", "2Jd", "2L5", "2Lo", "2Mf", "2Ps", "2RV", "2TG", "2Ti",
            "2U7", "2Uj", "2Xf", "2bH", "2dP", "2fA", "2fx", "2lW", "2nh", "2nz", "2rH", "2vK", "2xQ", "326", "34f", "354", "35U", "37A", "3AR",
            "3D9", "3JK", "3Ls", "3N3", "3PE", "3PI", "3Pi", "3Rd", "3Ta", "3Ue", "3ZS", "3ac", "3dM", "3fA", "3jD", "3ms", "3pH", "3tL", "3uC",
            "3vy", "3zE", "40M", "40Y", "40h", "40n", "416", "44L", "469", "46q", "471", "48z", "4A4", "4BF", "4BQ", "4Ex", "4HD", "4MK", "4Ql", "4RC",
            "4Tz", "4Xq", "4ZN", "4Ze", "4Zo", "4dT", "4di", "4gj", "4h6", "4mn", "4tM", "4wx", "51U", "53R", "552", "585", "5Da", "5Dz", "5Fc",
            "5Fq", "5Ia", "5JX", "5Mx", "5QX", "5Ra", "5Tu", "5X2", "5Yv", "5Zr", "5br", "5bs", "5dx", "5gx", "5iY", "5jp", "5mX", "5nF", "5oR",
            "5px", "5rX", "5tA", "5vT", "5xb", "60M", "611", "623", "62a", "62l", "64c", "64e", "67D", "68M", "695", "6Ba", "6J1", "6Kb", "6Pb", "6Pm",
            "6Sn", "6TI", "6U8", "6VO", "6XA", "6a6", "6iT", "6m0", "6oz", "6p7", "6pQ", "6t9", "6ts", "6uX", "6uk", "6vK", "6x3", "6xm", "6xy",
            "6yH", "6ys", "6zq", "70B", "70w", "72u", "76U", "76Y", "776", "78k", "7Dk", "7PJ", "7VX", "7XI", "7Za", "7fd", "7hF", "7i5", "7kK",
            "7rA", "7rV", "80U", "82G", "83S", "88c", "8BT", "8CS", "8HZ", "8P9", "8Qd", "8RO", "8RT", "8Tl", "8Vg", "8X8", "8Yq", "8ZJ", "8d8", "8e2",
            "8ev", "8he", "8jG", "8kF", "8l0", "8l4", "8nS", "8nb", "8ng", "8oo", "8tv", "8x6", "8zr", "92X", "94c", "97y", "980", "98I", "999",
            "99A", "99U", "9AQ", "9Bw", "9F6", "9FL", "9Fy", "9GC", "9H5", "9KD", "9N0", "9NK", "9Qw", "9RT", "9SR", "9Tv", "9Ue", "9XL", "9XY",
            "9Z6", "9ZE", "9aj", "9am", "9bU", "9dW", "9f9", "9gU", "9h6", "9hT", "9is", "9jh", "9oS", "9pw", "9tf", "9yG", "A0r", "A1L", "A1m", "A5R",
            "A8D", "AAY", "AC1", "ADV", "AFS", "AGK", "AHE", "AHP", "AHl", "AHu", "AKu", "ALZ", "ANN", "AS4", "AVY", "AVt", "AW4", "AXB", "AXK",
            "AXt", "AYT", "AaP", "AdD", "AdZ", "Afo", "Akm", "Akv", "AlD", "AmO", "Apt", "Aqx", "Aqz", "Asq", "AuD", "B2H", "B6C", "B8C", "BA9",
            "BJf", "BKh", "BLP", "BNh", "BP2", "BRh", "BTV", "BZo", "BZx", "BbH", "BcF", "Bfu", "Bi8", "BiL", "Bj0", "BkM", "Bky", "Bn8", "Bop", "Bte",
            "Bxw", "Bz8", "C0w", "C5K", "CA5", "CAs", "CBn", "CBv", "CE4", "CFL", "CGm", "CHI", "CIo", "CJ9", "CJX", "CMq", "COA", "COO", "CPM",
            "CPw", "CRC", "CSy", "CVM", "CWa", "CZT", "Cbp", "CdB", "Cek", "Cg0", "Cju", "Ck7", "ClI", "CmF", "Cn6", "CoH", "Cp6", "CsX", "CtI",
            "CvS", "CwE", "Cyv", "Cze", "D1Z", "D6Y", "DAL", "DDO", "DEd", "DFO", "DFj", "DG6", "DGr", "DIS", "DJG", "DL9", "DM6", "DNa", "DRG", "DU6",
            "DXB", "DXk", "Da3", "DbV", "Dbm", "Dbp", "Dd3", "Dde", "Dfx", "Dgi", "DhY", "Di0", "DjI", "Dmd", "Dn8", "Duj", "DvY", "Dx6", "Dxl",
            "E2V", "E2h", "E66", "E6w", "E7r", "E8k", "EBL", "EBd", "EFi", "EFx", "EJa", "EMo", "ENw", "EPx", "ESH", "ETG", "ETP", "ETh", "EVH",
            "EVr", "EVy", "EXK", "EXa", "EZH", "EbI", "EeF", "Ehh", "Epd", "Eqf", "Eto", "F0M", "F0P", "F3R", "F3w", "F6O", "F9o", "FAC", "FAV", "FBg",
            "FHm", "FJe", "FK1", "FLo", "FMO", "FMP", "FNA", "FSU", "FT7", "FZ2", "FZH", "FZh", "FZq", "FaB", "Fb4", "FbV", "FdF", "Fdl", "Fgb",
            "Fgg", "Fpb", "Frb", "Fw1", "Fw8", "G01", "G49", "G6z", "G9b", "GA7", "GBH", "GCC", "GCu", "GEs", "GVc", "GZJ", "GZl", "GZv", "GbZ",
            "GeP", "Gf9", "GfQ", "GhJ", "Ghy", "Gl5", "GmC", "Gmx", "Gnw", "GoA", "Grs", "GtT", "Gy2", "Gzz", "H0n", "H4E", "H5p", "H61", "H91", "HAe",
            "HBy", "HBz", "HDj", "HJy", "HLY", "HRZ", "HUx", "HVC", "Ha0", "HaC", "HeK", "HfH", "HgM", "HiP", "HnU", "HoD", "Hrk", "Hw2", "Hzz",
            "I3y", "I4k", "I58", "I6R", "I9G", "IB0", "IF2", "IFL", "IHe", "IJq", "IKJ", "ING", "ITR", "IU9", "IVE", "IWg", "IWh", "IX7", "IYB",
            "IZ8", "IcM", "Idp", "Ifr", "Ifz", "Ihb", "Iiw", "IjV", "Il5", "Ilr", "InJ", "IoP", "IqE", "Is7", "Iss", "J42", "J4K", "J61", "JAm", "JBf",
            "JDJ", "JEs", "JHs", "JJH", "JN9", "JPI", "JPj", "JTJ", "JTa", "JVR", "JXg", "Jcr", "Jhu", "JiH", "Jl0", "Jl2", "Jmu", "Jn7", "JtC",
            "Jv2", "JvJ", "Jy6", "K0E", "K27", "K9Q", "KFF", "KJt", "KLk", "KOQ", "KTb", "KVP", "KX4", "KXv", "Kdq", "KeU", "KeV", "KfB", "KhD",
            "Kjq", "Klp", "KmK", "Kn0", "KpI", "Kqv", "KsW", "Ksq", "Kzy", "L0T", "L0j", "L2B", "L2a", "L3w", "L45", "L6G", "L6h", "L7t", "L8Z", "LBQ",
            "LD7", "LGO", "LJ0", "LKw", "LKx", "LQQ", "LR6", "LXj", "LbU", "LbW", "Lcx", "Lgx", "LiP", "LiQ", "Ljv", "LsZ", "M10", "M1K", "M1Z",
            "M2i", "M2x", "M43", "M5S", "M5u", "M6P", "M6R", "M6g", "M87", "M9Y", "MBu", "MC1", "MCc", "MHN", "MHP", "MIy", "MJF", "MLx", "MMJ",
            "MND", "MPv", "MQ8", "MQ9", "MRy", "MSg", "MTE", "MTo", "MV0", "MVu", "MXd", "MdH", "Mgj", "MjV", "MkI", "Mme", "Mmf", "MqS", "MrC", "MrT",
            "Msi", "MtI", "Mtj", "MxL", "Mz9", "N66", "NBJ", "NDC", "NE9", "NFG", "NG3", "NIJ", "NIj", "NPz", "NQ7", "NSB", "NTA", "NVO", "NZd",
            "NcF", "Nf3", "NhJ", "Nhy", "Nib", "NjW", "Nke", "NlB", "NmH", "NnB", "Npd", "Nrz", "Nv9", "Nz4", "O1S", "O3E", "O3w", "O4x", "O8n",
            "OCB", "ODX", "ODx", "OIn", "OKk", "OMR", "OMv", "ONb", "OOH", "OPW", "OPj", "OQc", "OTg", "OVk", "OZz", "Ocm", "Od3", "Odi", "Oe3", "OfW",
            "Ogt", "Oo7", "Or2", "OtU", "Ovz", "P08", "P15", "P2I", "P2O", "P48", "P65", "P6x", "P8Q", "P9o", "PBQ", "PDq", "PFU", "PFg", "PFy",
            "PGK", "PJp", "PM5", "PMb", "PNN", "POv", "PRk", "PSe", "PYy", "PZj", "PfC", "Pff", "Ph0", "Pju", "Pjx", "Pl7", "PnO", "PtM", "Q0A",
            "Q3N", "Q3w", "Q4Z", "Q6l", "Q8N", "Q8t", "Q9i", "QAr", "QBz", "QF8", "QFd", "QHB", "QIS", "QMU", "QNf", "QOO", "QRO", "QVb", "QWh", "Qcm",
            "Qcs", "QdK", "Qde", "Qfp", "Qfx", "Qg6", "QgZ", "Qgc", "Qjm", "Qr2", "Qr9", "QrZ", "Qrk", "QtI", "QxS", "QxZ", "Qz5", "Qzx", "R0Y",
            "R2H", "R2O", "R4G", "R4U", "R4w", "R5W", "R8U", "RBS", "RBW", "RC3", "RFP", "RFT", "RL7", "RLR", "RNM", "RNU", "RQV", "RR6", "RRd",
            "RSK", "RSc", "RUw", "RVg", "RVk", "RXz", "RYp", "RZC", "RZS", "RZV", "RZu", "RdZ", "RfP", "Rgh", "Rhw", "RjG", "Rlc", "Rle", "RpL", "Rvk",
            "Rwk", "Rxa", "Rxt", "RyL", "Ryz", "RzC", "S0Z", "SCs", "SGR", "SIJ", "SIV", "SJK", "SJa", "SKq", "SNz", "SO3", "SRo", "SW4", "SZy",
            "Sct", "Scv", "SfS", "ShU", "SjS", "SlY", "SnZ", "Soq", "Spr", "SrH", "SrO", "St4", "SvT", "Swh", "T2f", "T2y", "T9m", "TBF", "TFd",
            "TIa", "TJ6", "TKK", "TLI", "TLd", "TLo", "TLw", "TMa", "TNh", "TQp", "TRA", "TRQ", "TTW", "TUp", "TVE", "TVO", "TVU", "TXG", "TZ0", "TZa",
            "Tdc", "TeI", "Tej", "TfK", "Thw", "Tjb", "Tk1", "Tk3", "Tl3", "Tln", "TnB", "Tp8", "TpQ", "Tqr", "Tzc", "Tzq", "U28", "U2I", "U7M",
            "U8Y", "U8w", "UBE", "UBe", "UHA", "UMD", "UOp", "UUR", "UXG", "UYr", "UaL", "UbF", "UbI", "UbM", "Uby", "Uk8", "Umw", "Uoy", "UrZ",
            "UtM", "Uth", "Uu0", "UvZ", "Uwe", "V04", "V2k", "V4B", "V4V", "V4v", "V5K", "V7N", "V8K", "V8U", "V9d", "VBu", "VDB", "VJq", "VKm", "VLQ",
            "VOl", "VQo", "VRY", "VTU", "VTn", "VVd", "VWu", "VX0", "VXJ", "VXW", "VXZ", "VYA", "Vdj", "Vem", "VfD", "Vlv", "Vpt", "VxX", "W1d",
            "W4l", "W86", "W8Q", "W8c", "WAX", "WDD", "WF0", "WJX", "WQ8", "WR2", "WRD", "WRq", "WVB", "WZH", "Wdz", "We1", "Wew", "Wfw", "Wg5",
            "WjP", "Wki", "WmZ", "Wn8", "WpZ", "WrG", "Wrk", "Wv1", "Wxt", "X2o", "X2x", "X4G", "X6F", "XB3", "XBu", "XE7", "XEA", "XL1", "XNw", "XOJ",
            "XUh", "XUv", "XVY", "XW5", "XW8", "XXf", "XZn", "XbB", "Xbj", "XfR", "Xfw", "XgN", "XjN", "Xo0", "XpL", "Xpm", "XqN", "XrY", "XtA",
            "Xug", "XvT", "Xwt", "Xxx", "Y2R", "Y31", "Y5F", "YA4", "YCt", "YDt", "YFI", "YGX", "YGy", "YKx", "YNA", "YPN", "YRv", "YUY", "YYI",
            "Yb1", "Ybx", "Yd6", "YdD", "YfH", "Yfc", "YgQ", "YhM", "Yix", "Yjv", "YlO", "Ylu", "YnX", "Yne", "Yos", "YrB", "Yso", "Ysy", "YtL", "Z2o",
            "Z49", "Z6U", "Z75", "Z8f", "Z8s", "ZAQ", "ZD4", "ZDT", "ZDj", "ZFv", "ZHt", "ZJc", "ZNE", "ZO8", "ZRO", "ZUp", "ZVO", "Zc5", "ZcV",
            "ZfZ", "Zj1", "ZlR", "Znp", "Zpy", "Zsm", "Zwa", "Zyd", "Zym", "Zz1", "Zz8", "a1F", "a4X", "a4j", "a5q", "aA2", "aBO", "aBe", "aE9",
            "aGj", "aGz", "aHD", "aJ1", "aJz", "aLl", "aYr", "aa3", "af1", "arC", "arE", "asN", "asW", "auX", "ax3", "b0V", "b24", "b2X", "b6A", "bHa",
            "bOA", "bOt", "bTj", "bVQ", "bVz", "bY1", "bZm", "bdm", "bmg", "bpc", "bqN", "bqq", "bw7", "bwj", "bz3", "bz4", "bzd", "c0y", "c1y",
            "c20", "c2B", "c2X", "c2b", "c8H", "cBj", "cG3", "cIR", "cJW", "cNA", "cNG", "cPl", "cQH", "cUy", "cVU", "cWc", "cXa", "cZ6", "caU",
            "cbF", "cdX", "cdj", "cfZ", "chI", "cjf", "cl3", "ctC", "cvX", "cw1", "cyg", "d0c", "d19", "d2h", "d4t", "d5R", "dAj", "dBa", "dC3", "dEq",
            "dFG", "dFm", "dJb", "dNC", "dPr", "dVh", "dWY", "dc2", "djW", "dkZ", "dlw", "dmV", "dve", "e0H", "e1H", "e1t", "e2j", "e6m", "e6v",
            "eHW", "eHu", "eLV", "ePr", "eWA", "efO", "eh5", "eim", "eiu", "en5", "enq", "epQ", "et6", "etf", "evf", "exc", "ezE", "f0c", "f4W",
            "f4d", "f5c", "fB1", "fBX", "fD2", "fDF", "fDf", "fE5", "fH2", "fJ4", "fKR", "fQM", "fRH", "fV7", "fVT", "fX7", "fZt", "fa5", "faW", "fb4",
            "fda", "fek", "ffQ", "fhR", "fhe", "fjL", "fjU", "flF", "fm3", "fpR", "fpc", "fqL", "fqT", "fsO", "fsZ", "ftf", "fu3", "fxy", "fzW",
            "g3l", "g4V", "g4b", "g7T", "gB0", "gBH", "gBN", "gDB", "gHi", "gJR", "gNg", "gRN", "gSL", "gV2", "gbB", "gbo", "gct", "gk5", "gmS",
            "go4", "guW", "gxi", "h0l", "h4l", "h5t", "h8k", "h8s", "hDD", "hDm", "hHc", "hNK", "hPV", "hPq", "hRO", "hSa", "hX6", "hXn", "hYB", "hcf",
            "hcw", "heN", "hfO", "hgv", "hkR", "hkv", "hlA", "hmp", "hp3", "hqv", "hrB", "hsr", "hzP", "hzi", "i2E", "i4b", "i4t", "i5O", "i6F",
            "i6G", "i6I", "i76", "i7P", "i8u", "i9o", "iAm", "iB4", "iBv", "iP9", "iRY", "iSI", "iSJ", "iUh", "iUx", "iVA", "iWb", "iXM", "iXT",
            "iZT", "iZY", "ibm", "idO", "if9", "igr", "ijX", "ilH", "ilt", "ilw", "imb", "iov", "ipI", "irp", "isD", "ivR", "ivk", "j1o", "j57", "j5T",
            "j7W", "j8N", "j8Y", "j8y", "jDB", "jGe", "jHm", "jIw", "jKY", "jNA", "jRP", "jVa", "jXl", "jXp", "jXr", "jYY", "jZR", "ja7", "jd4",
            "jdq", "jf3", "jgY", "jiD", "jkH", "jlE", "jlQ", "jnL", "jno", "jri", "k20", "k4N", "k4y", "kDm", "kGi", "kIZ", "kJ5", "kLB", "kPU",
            "kPt", "kQE", "kSl", "kVI", "kVO", "kWl", "kZE", "kh2", "kji", "klJ", "kmz", "ktX", "kzj", "l4V", "l7g", "l8p", "l8u", "l9P", "l9R", "lDu",
            "lKs", "lMf", "lN0", "lPm", "lPx", "lab", "las", "lb5", "lks", "lmp", "lqp", "lrD", "lrl", "ltY", "luu", "lwU", "lxf", "lyA", "m0U",
            "m4X", "m4n", "m54", "m5f", "m5q", "m6k", "m88", "mCy", "mD6", "mDp", "mFD", "mHc", "mJK", "mJw", "mNa", "mNe", "mOU", "mPz", "mRY",
            "mTV", "mTa", "mUo", "mUx", "mVX", "mWR", "mWd", "mXD", "mYN", "maM", "mc6", "mdS", "mdw", "mfR", "mhW", "mjj", "mkq", "moL", "mpd", "mrU",
            "mra", "msK", "mtz", "mvc", "mvr", "mzr", "n3W", "n4P", "n4c", "n4m", "n7z", "n97", "nBX", "nBc", "nDR", "nF3", "nJP", "nJx", "nKs",
            "nLs", "nNU", "nOy", "nPk", "nQm", "nZO", "nbi", "ncC", "ncL", "ndE", "nds", "nfZ", "nfm", "njG", "njZ", "njb", "nrk", "nxR", "nyc",
            "nz2", "o0L", "o1D", "o3d", "o4q", "o6P", "o6R", "o7H", "o81", "o8f", "oDM", "oER", "oFd", "oFg", "oGC", "oIO", "oLs", "oMr", "oRP", "oSt",
            "oUq", "oXq", "oXu", "oZk", "obH", "oek", "ofX", "okE", "omi", "op7", "opd", "oqF", "ouW", "oyf", "ozW", "p0D", "p0i", "p49", "p8J",
            "pBD", "pDP", "pGX", "pJM", "pN4", "pPB", "pPa", "pRg", "pRv", "pSN", "pT7", "pY6", "pYj", "pbh", "pgu", "phM", "plE", "pmv", "pnr",
            "poZ", "pob", "pot", "pv5", "q0D", "q0j", "q2v", "qJD", "qLe", "qMh", "qMi", "qOA", "qOc", "qRG", "qSk", "qUx", "qVM", "qWd", "qZ5", "qeS",
            "qet", "qfs", "qhf", "qht", "qkN", "qkp", "qmQ", "qnC", "qoJ", "qof", "qqG", "qsS", "qvM", "qxQ", "r2k", "r4q", "rB8", "rGa", "rIw",
            "rK6", "rKc", "rL1", "rLW", "rQB", "rQc", "rQv", "rTR", "rXw", "rYJ", "rZo", "rhW", "rha", "rmo", "roH", "rpA", "rpm", "rqn", "rt7",
            "ryj", "rzC", "rzN", "s2c", "s8w", "sDB", "sDV", "sE7", "sHa", "sHb", "sHd", "sIi", "sJD", "sJZ", "sNd", "sPW", "sPq", "sQM", "sRp", "sRv",
            "sTE", "sX1", "sZ4", "sbX", "sbc", "se9", "skE", "skV", "slC", "slH", "spd", "sq3", "srD", "sre", "sxM", "szv", "t1J", "t4X", "t4m",
            "t9s", "tBa", "tDJ", "tGG", "tGb", "tLm", "tTE", "tV4", "tXV", "tZr", "tfw", "tho", "tip", "tmK", "tnB", "tzO", "u00", "u0k", "u25",
            "u2z", "u38", "uCp", "uFD", "uFl", "uHX", "uLB", "uLe", "uN9", "uPJ", "uQ2", "uVE", "uZr", "ueI", "uf0", "uiz", "ukn", "uoT", "uqP", "uqU",
            "uqa", "uth", "utj", "uz0", "v2Z", "v5t", "v95", "vAD", "vAL", "vHg", "vIk", "vIx", "vKr", "vLK", "vLm", "vMZ", "vMl", "vP1", "vPg",
            "vPi", "vQY", "vRA", "vbN", "vbl", "vbw", "vdj", "ve5", "vkN", "vly", "vps", "vrD", "vs6", "vtF", "vxj", "vy6", "w0B", "w2I", "w4e",
            "w7W", "w7t", "wAS", "wAe", "wDj", "wFk", "wHV", "wHs", "wJy", "wLu", "wLz", "wM3", "wNS", "wPl", "wR8", "wUx", "wZR", "waZ", "wac", "wae",
            "wfH", "wgM", "wjV", "wjZ", "wl0", "wmP", "woZ", "wpu", "wrU", "wtH", "wvh", "wwQ", "wxD", "x2O", "x2R", "x5u", "x88", "xAU", "xB1",
            "xB3", "xCk", "xDY", "xFA", "xFm", "xJn", "xKP", "xNm", "xVg", "xXH", "xXd", "xZA", "xb4", "xbw", "xcv", "xeC", "xlh", "xpY", "xpd",
            "xsz", "xvP", "xxY", "y0j", "y1p", "y2r", "y48", "y4m", "y85", "y8t", "yBh", "yDp", "yEA", "yG3", "yGV", "yH2", "yH8", "yHq", "yLE", "yLo",
            "yMY", "yPE", "yPs", "yRQ", "ySJ", "yU1", "yZ9", "yZL", "yZV", "yZi", "yd2", "yfH", "yfa", "ygL", "yk2", "ykU", "yl6", "ylv", "yso",
            "yuC", "yxc", "yzb", "yzk", "z3C", "z4a", "z5F", "z6w", "zEp", "zJX", "zKr", "zLh", "zNk", "zNx", "zPo", "zRC", "zSw", "zTl", "zU8",
            "zUg", "zWe", "zWy", "zXf", "zfi", "zhD", "zhr", "zjx", "zlg", "znA", "znH", "zv7", "zzk" };
        //String tenant = "999"; //brewspace
        //String tenant = "Z49"; //big tenant

        //MiruTenantId tenantId = new MiruTenantId(tenant.getBytes(Charsets.UTF_8));

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
            .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());

        final RequestHelper[] requestHelpers = new RequestHelper[REMOTE_HOSTS.length];
        for (int i = 0; i < REMOTE_HOSTS.length; i++) {
            String remoteHost = REMOTE_HOSTS[i];
            requestHelpers[i] = new RequestHelper(httpClientFactory.createClient(remoteHost, REMOTE_PORT), objectMapper);
        }

        final MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and,
            Optional.of(Arrays.asList(
                new MiruFieldFilter("objectType", Lists.transform(
                    Arrays.asList(102, 1, 18, 38, 801, 1_464_927_464, -960_826_044),
                    Functions.toStringFunction())),
                new MiruFieldFilter("activityType", Lists.transform(Arrays.asList(
                    0, //viewed
                    11, //liked
                    1, //created
                    65 //outcome_set
                ), Functions.toStringFunction()))
            )),
            Optional.<List<MiruFilter>>absent());

        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
        final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
        final long packThreeDays = snowflakeIdPacker.pack(TimeUnit.DAYS.toMillis(30), 0, 0);

        ExecutorService executorService = Executors.newFixedThreadPool(8);
        int numQueries = 10_000;
        final Random rand = new Random();
        for (int i = 0; i < numQueries; i++) {
            final int index = i;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    MiruTenantId tenantId = new MiruTenantId(tenants[index % tenants.length].getBytes(Charsets.UTF_8));
                    MiruRequest<TrendingQuery> query = new MiruRequest<>(tenantId, new MiruActorId(new Id(3_765)),
                        MiruAuthzExpression.NOT_PROVIDED,
                        new TrendingQuery(
                            new MiruTimeRange(packCurrentTime - packThreeDays, packCurrentTime),
                            32,
                            constraintsFilter,
                            "parent",
                            100), true);

                    @SuppressWarnings("unchecked")
                    MiruResponse<OldTrendingAnswer> response = requestHelpers[rand.nextInt(requestHelpers.length)].executeRequest(query,
                        TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT,
                        MiruResponse.class, new Class[] { OldTrendingAnswer.class }, null);
                    /*
                    if (response.totalElapsed > 100) {
                        System.out.println("tenantId=" + tenantId);
                    }
                    */
                    System.out.println("tenantId: " + tenantId + ", index: " + index + ", totalElapsed: " + response.totalElapsed);
                    assertNotNull(response);
                }
            });
        }

        executorService.shutdown();
        long start = System.currentTimeMillis();
        while (!executorService.isTerminated()) {
            System.out.println("Awaiting completion for " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test(enabled = false, description = "Needs REMOTE constants")
    public void testSystemRecommended() throws Exception {

        String tenant = "999"; //brewspace
        //String tenant = "Z49"; //big tenant
        MiruTenantId tenantId = new MiruTenantId(tenant.getBytes(Charsets.UTF_8));

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
            .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());

        final RequestHelper[] requestHelpers = new RequestHelper[REMOTE_HOSTS.length];
        for (int i = 0; i < REMOTE_HOSTS.length; i++) {
            String remoteHost = REMOTE_HOSTS[i];
            requestHelpers[i] = new RequestHelper(httpClientFactory.createClient(remoteHost, REMOTE_PORT), objectMapper);
        }

        MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and,
            Optional.of(Arrays.asList(
                new MiruFieldFilter("user", Arrays.asList(String.valueOf(3_181))), //   3765  2902 3251 3816 3181 5723
                    /*new MiruFieldFilter("objectType", Lists.transform(
                            Arrays.asList(102, 1, 18, 38, 801, 1464927464, -960826044),
                            Functions.toStringFunction())),*/
                new MiruFieldFilter("activityType", Lists.transform(Arrays.asList(
                        0 //viewed
                        //11, //liked
                        //1, //created
                        //65 //outcome_set
                    ),
                    Functions.toStringFunction()))
            )),
            Optional.<List<MiruFilter>>absent());

        MiruFilter resultConstraintFilter = new MiruFilter(MiruFilterOperation.and,
            Optional.of(Arrays.asList(
                new MiruFieldFilter("objectType", Lists.transform(Arrays.asList(102), Functions.toStringFunction())))),
            Optional.<List<MiruFilter>>absent());


        MiruRequest<RecoQuery> request = new MiruRequest<>(tenantId,
            new MiruActorId(new Id(3_765)),
            MiruAuthzExpression.NOT_PROVIDED,
            new RecoQuery(constraintsFilter,
                "parent", "parent", "parent",
                "user", "user", "user",
                "parent", "parent",
                resultConstraintFilter,
                100), true);

        Random rand = new Random();
        @SuppressWarnings("unchecked")
        MiruResponse<RecoAnswer> recoAnswer = requestHelpers[rand.nextInt(requestHelpers.length)].executeRequest(request,
            RecoConstants.RECO_PREFIX + RecoConstants.CUSTOM_QUERY_ENDPOINT,
            MiruResponse.class, new Class[] { RecoAnswer.class }, null);
        System.out.println(recoAnswer);
        System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(recoAnswer));
        assertNotNull(recoAnswer);
    }

}
