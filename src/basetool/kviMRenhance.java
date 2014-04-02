//package basetool;
//
//public class kviMRenhance {
//
//}

package basetool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

@SuppressWarnings("unused")
public class kviMRenhance {

	// double lb, rb;
	public static int modorder;
	public static double numMapIn;
	public static double numMapOut;
	// public static int rednum;
	public String tabname;

	// for temp
	public int scanCache; // = 200;//100000;
	public int nored; // = 16;

	// long[] relreg = new long[100];
	// int relregnum = 0;
	int[] relregFlag = new int[100];
	public static String prekey = "abcdefghijklmnopqrstuvwxyz";

	// for val
	// public int scanCache = 150;//1500;//100000;
	// public int nored = 16;

	int cnt = 0;

	public kviMRenhance(int modelord, String tabidx, int sign) {// 0: temp 1:val
		modorder = modelord;
		tabname = tabidx;
		numMapIn = 0;
		numMapOut = 0;

		// relregnum = 0;

		for (int i = 0; i < 100; ++i)
			relregFlag[i] = 0;

		if (sign == 0)// temp
		{
			// scanCache = 350;//300;//175;//170;//170;//200;//100000;

			// scanCache = 650, 700, 1000;
			scanCache = 1000;
			nored = 16;
		} else// value
		{
			// scanCache = 155;// 1500;//100000;
			scanCache = 3;
			nored = 16;
		}
	}

	public static String regvalKeyCon(long regval) {
		long tmp = regval;
		int num = 0;
		while (tmp != 0) {
			tmp = tmp / 10;
			num++;
		}
		if (num == 0)
			num = 1;
		return prekey.charAt(num - 1) + "," + Long.toString(regval);

	}

	public void getRelReg(long regid[], int num) {

		// for(int i=0;i<100;++i)
		// relregFlag[i]=0;

		for (int i = 0; i < num; ++i) {
			// relregFlag[(int)regid[i]]=1;

			// relreg[i] = regid[i];

		}
		// relregnum = num;
		return;
	}

	public static class RegMergeTableInputFormat_test extends TableInputFormat {
		// the whole region with relevant node is for one split

		// final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

		public static int staRelregnum = 0;
		public static long[] staRelreg = new long[200];

		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException {

			int j = 0;
			// int tmpcnt = 0;
			long[] selReg = new long[200];
			int selRegCnt = 0;

			HTable table = this.getHTable(); // maybe super.getHTable
			Scan scan = this.getScan(); // maybe super.getHTable

			if (table == null) {
				throw new IOException("No table was provided.");
			}
			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}
			// int count = 0;

			List<InputSplit> splits = new ArrayList<InputSplit>(
					keys.getFirst().length);

			String regionLocation;
			HRegionInfo reginfor;
			long tmpid = 0;

			int flag = 0;

			byte[] splitStart = keys.getFirst()[0];
			byte[] splitStop = keys.getSecond()[0];

			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}

				regionLocation = table.getRegionLocation(keys.getFirst()[i])
						.getHostname();

				reginfor = table.getRegionLocation(keys.getFirst()[i])
						.getRegionInfo();

				tmpid = reginfor.getRegionId();

				if (flag == 0) {

					splitStart = keys.getFirst()[i];
					flag = 1;

				} else {

					splitStop = keys.getSecond()[i];
					InputSplit split = new TableSplit(table.getTableName(),
							splitStart, splitStop, regionLocation);
					splits.add(split);
					flag = 0;

				}

				//
				// byte[] startRow = scan.getStartRow();
				// byte[] stopRow = scan.getStopRow();
				// determine if the given start an stop key fall into the region
				// if ((startRow.length == 0 || keys.getSecond()[i].length == 0
				// || Bytes
				// .compareTo(startRow, keys.getSecond()[i]) < 0)
				// && (stopRow.length == 0 || Bytes.compareTo(stopRow,
				// keys.getFirst()[i]) > 0)) {

				// if (LOG.isDebugEnabled())
				// LOG.debug("getSplits: split -> " + (count++) + " -> " +
				// split);

				// selReg[selRegCnt++] = tmpid;

				// }
			}

			// ........test...................//
			System.out.printf("the number of splits: %d\n", splits.size());

			// ...............................//

			return splits;

			// List<InputSplit> splits = new ArrayList<InputSplit>();
			// Scan scan = getScan();
			// byte startRow[] = scan.getStartRow(), stopRow[] =
			// scan.getStopRow();
			// byte prefixedStartRow[] = new byte[startRow.length + 1];
			// byte prefixedStopRow[] = new byte[stopRow.length + 1];
			// System.arraycopy(startRow, 0, prefixedStartRow, 1,
			// startRow.length);
			// System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
			//
			// for (int prefix = -128; prefix < 128; prefix++) {
			// prefixedStartRow[0] = (byte) prefix;
			// prefixedStopRow[0] = (byte) prefix;
			// scan.setStartRow(prefixedStartRow);
			// scan.setStopRow(prefixedStopRow);
			// setScan(scan);
			// splits.addAll(super.getSplits(context));
			// }

			// return splits;
		}
	}

	public static class RegTableInputFormat extends TableInputFormat {
		// the whole region with relevant node is for one split

		// final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

		public static int staRelregnum = 0;
		public static long[] staRelreg = new long[200];

		public static void getRegion(int num, long regid[]) {

			// System.out.printf("regions of interest: ");

			for (int i = 0; i < num; ++i) {
				staRelreg[i] = regid[i];
				// System.out.printf(" %d,",regid[i]);
			}
			staRelregnum = num;

			// System.out.printf("\n");
			return;
		}

		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException {

			int j = 0;
			// int tmpcnt = 0;
			long[] selReg = new long[200];
			int selRegCnt = 0;

			HTable table = this.getHTable(); // maybe super.getHTable
			Scan scan = this.getScan(); // maybe super.getHTable

			if (table == null) {
				throw new IOException("No table was provided.");
			}
			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}
			// int count = 0;

			List<InputSplit> splits = new ArrayList<InputSplit>(
					keys.getFirst().length);

			String regionLocation;
			HRegionInfo reginfor;
			long tmpid = 0;

			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}

				regionLocation = table.getRegionLocation(keys.getFirst()[i])
						.getHostname();

				reginfor = table.getRegionLocation(keys.getFirst()[i])
						.getRegionInfo();

				tmpid = reginfor.getRegionId();

				byte[] startRow = scan.getStartRow();
				byte[] stopRow = scan.getStopRow();
				// determine if the given start an stop key fall into the region
				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
						.compareTo(startRow, keys.getSecond()[i]) < 0)
						&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
								keys.getFirst()[i]) > 0)) {
					byte[] splitStart = startRow.length == 0
							|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
							.getFirst()[i] : startRow;
					byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
							keys.getSecond()[i], stopRow) <= 0)
							&& keys.getSecond()[i].length > 0 ? keys
							.getSecond()[i] : stopRow;
					InputSplit split = new TableSplit(table.getTableName(),
							splitStart, splitStop, regionLocation);
					splits.add(split);
					// if (LOG.isDebugEnabled())
					// LOG.debug("getSplits: split -> " + (count++) + " -> " +
					// split);

					selReg[selRegCnt++] = tmpid;

				} else {
					// //eliminate current split
					for (j = 0; j < staRelregnum; ++j) {
						if (staRelreg[j] == tmpid) {
							break;
						}

					}
					if (j == staRelregnum)
						continue;
					else {
						int k = 0;
						for (k = 0; k < selRegCnt; ++k) {
							if (selReg[k] == tmpid) {
								break;
							}
						}
						if (k == selRegCnt) {
							selReg[selRegCnt++] = tmpid;

							InputSplit split = new TableSplit(
									table.getTableName(), keys.getFirst()[i],
									keys.getSecond()[i], regionLocation);
							splits.add(split);

						} else {
							continue;
						}
					}
				}
			}

			// ........test...................//
			System.out.printf("the number of splits: %d\n", splits.size());

			// ...............................//

			return splits;

			// List<InputSplit> splits = new ArrayList<InputSplit>();
			// Scan scan = getScan();
			// byte startRow[] = scan.getStartRow(), stopRow[] =
			// scan.getStopRow();
			// byte prefixedStartRow[] = new byte[startRow.length + 1];
			// byte prefixedStopRow[] = new byte[stopRow.length + 1];
			// System.arraycopy(startRow, 0, prefixedStartRow, 1,
			// startRow.length);
			// System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
			//
			// for (int prefix = -128; prefix < 128; prefix++) {
			// prefixedStartRow[0] = (byte) prefix;
			// prefixedStopRow[0] = (byte) prefix;
			// scan.setStartRow(prefixedStartRow);
			// scan.setStopRow(prefixedStopRow);
			// setScan(scan);
			// splits.addAll(super.getSplits(context));
			// }

			// return splits;
		}
	}

	public static class RegScanTableInputFormat extends TableInputFormat {
		// the whole region with relevant node is for one split

		// final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

		public static int staScanRelregnum = 0;
		public static long[] staScanRelreg = new long[200];

		// public static int staScanPerReg = 0;
		public static String[][] staScanPerReg = new String[200][3];

		public static void getRegion(int num, long regid[],
				String scanPerReg[][]) {

			// System.out.printf("regions of interest: ");

			for (int i = 0; i < num; ++i) {
				staScanRelreg[i] = regid[i];
				staScanPerReg[i][0] = scanPerReg[i][0];
				staScanPerReg[i][1] = scanPerReg[i][1];
				// System.out.printf(" %d,",regid[i]);
			}
			staScanRelregnum = num;

			// System.out.printf("\n");
			return;
		}

		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException {

			int j = 0;
			int tmpcnt = 0;
			long[] selReg = new long[200];
			int selRegCnt = 0;

			HTable table = this.getHTable(); // maybe super.getHTable
			Scan scan = this.getScan(); // maybe super.getHTable

			if (table == null) {
				throw new IOException("No table was provided.");
			}
			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}

			int relRegNum = 0;
			List<InputSplit> splits = new ArrayList<InputSplit>(
					keys.getFirst().length);

			String regionLocation;
			HRegionInfo reginfor;
			long tmpid = 0;
			int k = 0;
			int flag = 0;

			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}

				regionLocation = table.getRegionLocation(keys.getFirst()[i])
						.getHostname();

				reginfor = table.getRegionLocation(keys.getFirst()[i])
						.getRegionInfo();

				tmpid = reginfor.getRegionId();

				byte[] startRow = scan.getStartRow();
				byte[] stopRow = scan.getStopRow();

				byte[] splitStart = new byte[keys.getFirst()[0].length + 2];
				byte[] splitStop = new byte[keys.getFirst()[0].length + 2];

				// determine if the given start an stop key fall into the region
				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
						.compareTo(startRow, keys.getSecond()[i]) < 0)
						&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
								keys.getFirst()[i]) > 0)) {
					splitStart = startRow.length == 0
							|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
							.getFirst()[i] : startRow;
					splitStop = (stopRow.length == 0 || Bytes.compareTo(
							keys.getSecond()[i], stopRow) <= 0)
							&& keys.getSecond()[i].length > 0 ? keys
							.getSecond()[i] : stopRow;

					flag = 1;

					// InputSplit split = new TableSplit(table.getTableName(),
					// splitStart, splitStop, regionLocation);
					//
					// splits.add(split);
					// if (LOG.isDebugEnabled())
					// LOG.debug("getSplits: split -> " + (count++) + " -> " +
					// split);

					// for (k = 0; k < selRegCnt; ++k) {
					// if (selReg[k] == tmpid) {
					// break;
					// }
					// }
					// if (k == selRegCnt) {
					// selReg[selRegCnt++] = tmpid;

				}

				// //eliminate current split
				for (j = 0; j < staScanRelregnum; ++j) {
					if (staScanRelreg[j] == tmpid) {
						relRegNum = j;
						break;
					}

				}
				if (j == staScanRelregnum)
					continue;
				else {

					for (k = 0; k < selRegCnt; ++k) {
						if (selReg[k] == tmpid) {
							break;
						}
					}
					if (k == selRegCnt) {
						selReg[selRegCnt++] = tmpid;

						byte[] splitStart1 = Bytes
								.toBytes(staScanPerReg[relRegNum][0]);
						byte[] splitStop1 = Bytes
								.toBytes(staScanPerReg[relRegNum][1]);

						if (flag == 1) {

							splitStart = Bytes.compareTo(splitStart,
									splitStart1) >= 0 ? splitStart1
									: splitStart;

							splitStop = Bytes.compareTo(splitStop, splitStop1) >= 0 ? splitStop
									: splitStop1;

						} else {
							splitStart = splitStart1;
							splitStop = splitStop1;
						}

						InputSplit split1 = new TableSplit(
								table.getTableName(), splitStart, splitStop,
								regionLocation);

						splits.add(split1);

					} else {
						continue;
					}

				}
				flag = 0;

			}

			// ........test...................//
			System.out.printf("the number of splits: %d\n", splits.size());

			// ...............................//

			return splits;

			// List<InputSplit> splits = new ArrayList<InputSplit>();
			// Scan scan = getScan();
			// byte startRow[] = scan.getStartRow(), stopRow[] =
			// scan.getStopRow();
			// byte prefixedStartRow[] = new byte[startRow.length + 1];
			// byte prefixedStopRow[] = new byte[stopRow.length + 1];
			// System.arraycopy(startRow, 0, prefixedStartRow, 1,
			// startRow.length);
			// System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
			//
			// for (int prefix = -128; prefix < 128; prefix++) {
			// prefixedStartRow[0] = (byte) prefix;
			// prefixedStopRow[0] = (byte) prefix;
			// scan.setStartRow(prefixedStartRow);
			// scan.setStopRow(prefixedStopRow);
			// setScan(scan);
			// splits.addAll(super.getSplits(context));
			// }

			// return splits;
		}
	}

	public static class RegAdaBaseTableInputFormat extends TableInputFormat {
		// the whole region with relevant node is for one split

		// final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

		public static int staAdaBaseNum = 0;
		public static long[] staAdaBaseReg = new long[200];
		public static double[] staAdaBasePath = new double[200];
		// public static int[] staAdaBasePerRegCnt=new int[200];

		public static int cursplit = 0;
		public static int mapcons = 0;

		// public static int staScanPerReg = 0;
		// public static String[][] staAdaBasePerReg = new String[200][3];

		public static void getRegion(int num, long regid[], double nodepath[],
				int split, int cons) {

			// System.out.printf("regions of interest: ");

			for (int i = 0; i < num; ++i) {
				staAdaBaseReg[i] = regid[i];
				staAdaBasePath[i] = nodepath[i];
			}
			staAdaBaseNum = num;

			cursplit = split;
			mapcons = cons;

			// System.out.printf("\n");
			return;
		}

		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException {

			int j = 0;
			int tmpcnt = 0;
			long[] selReg = new long[200];
			int selRegCnt = 0;

			HTable table = this.getHTable(); // maybe super.getHTable
			Scan scan = this.getScan(); // maybe super.getHTable

			if (table == null) {
				throw new IOException("No table was provided.");
			}
			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}

			int relRegNum = 0;
			List<InputSplit> splits = new ArrayList<InputSplit>(
					keys.getFirst().length);

			String regionLocation;
			HRegionInfo reginfor;
			long tmpid = 0;

			byte[] splitStart = new byte[keys.getFirst()[0].length + 2];
			byte[] splitStop = new byte[keys.getFirst()[0].length + 2];

			byte[] startRow = scan.getStartRow();
			byte[] stopRow = scan.getStopRow();

			long[] consec = new long[100];
			int consecnum = 0;

			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}

				regionLocation = table.getRegionLocation(keys.getFirst()[i])
						.getHostname();

				reginfor = table.getRegionLocation(keys.getFirst()[i])
						.getRegionInfo();

				tmpid = reginfor.getRegionId();

				// determine if the given start an stop key fall into the region
				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
						.compareTo(startRow, keys.getSecond()[i]) < 0)
						&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
								keys.getFirst()[i]) > 0)) {
					consec[consecnum] = i;
					consecnum++;
				}
			}

			double groupk = 2.0, tmpsum = cursplit + consecnum;
			while ((tmpsum / groupk) > mapcons) {
				groupk++;
			}
			int groupcnt = 0, regcnt = keys.getFirst().length;

			String nodel = "", noder = "";

			for (int i = 0; i < staAdaBaseNum; ++i) {

				for (j = 0; j < regcnt; j++) {
					regionLocation = table
							.getRegionLocation(keys.getFirst()[i])
							.getHostname();

					reginfor = table.getRegionLocation(keys.getFirst()[i])
							.getRegionInfo();

					tmpid = reginfor.getRegionId();
					if (tmpid == staAdaBaseReg[i]) {
						break;
					}
				}
				if (j != regcnt) {
					groupcnt++;
				} else {
					continue;
				}
				if (groupcnt == 1) {

					nodel = regvalKeyCon((long) staAdaBasePath[i]) + ",0";
					noder = regvalKeyCon((long) (staAdaBasePath[i] + 1)) + ",0";
					splitStart = Bytes.toBytes(nodel);
					splitStop = Bytes.toBytes(noder);

				} else if (groupcnt < groupk) {
					nodel = regvalKeyCon((long) staAdaBasePath[i]) + ",0";
					noder = regvalKeyCon((long) (staAdaBasePath[i] + 1)) + ",0";
					splitStart = Bytes.compareTo(splitStart,
							Bytes.toBytes(nodel)) >= 0 ? Bytes.toBytes(nodel)
							: splitStart;

					splitStop = Bytes
							.compareTo(splitStop, Bytes.toBytes(noder)) >= 0 ? splitStop
							: Bytes.toBytes(noder);

				} else if (groupcnt == groupk) {

					regionLocation = table
							.getRegionLocation(keys.getFirst()[j])
							.getHostname();

					InputSplit split = new TableSplit(table.getTableName(),
							splitStart, splitStop, regionLocation);
					splits.add(split);

					groupcnt = 0;
				}

			}

			byte[] regStart = new byte[keys.getFirst()[0].length + 2];
			byte[] regStop = new byte[keys.getFirst()[0].length + 2];

			for (int i = 0; i < consecnum; ++i) {

				regStart = keys.getFirst()[i];
				regStop = keys.getSecond()[i];

				groupcnt++;

				if (groupcnt == 1) {

					splitStart = regStart;
					splitStop = regStop;

				} else if (groupcnt < groupk) {
					splitStart = Bytes.compareTo(splitStart, regStart) >= 0 ? regStart
							: splitStart;

					splitStop = Bytes.compareTo(splitStop, regStop) >= 0 ? splitStop
							: regStop;
				} else if (groupcnt == groupk) {

					regionLocation = table
							.getRegionLocation(keys.getFirst()[i])
							.getHostname();

					InputSplit split = new TableSplit(table.getTableName(),
							splitStart, splitStop, regionLocation);
					splits.add(split);
					groupcnt = 0;
				}
			}
			// ........test...................//
			System.out.printf("the number of splits: %d\n", splits.size());

			// ...............................//

			return splits;

			// List<InputSplit> splits = new ArrayList<InputSplit>();
			// Scan scan = getScan();
			// byte startRow[] = scan.getStartRow(), stopRow[] =
			// scan.getStopRow();
			// byte prefixedStartRow[] = new byte[startRow.length + 1];
			// byte prefixedStopRow[] = new byte[stopRow.length + 1];
			// System.arraycopy(startRow, 0, prefixedStartRow, 1,
			// startRow.length);
			// System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
			//
			// for (int prefix = -128; prefix < 128; prefix++) {
			// prefixedStartRow[0] = (byte) prefix;
			// prefixedStopRow[0] = (byte) prefix;
			// scan.setStartRow(prefixedStartRow);
			// scan.setStopRow(prefixedStopRow);
			// setScan(scan);
			// splits.addAll(super.getSplits(context));
			// }

			// return splits;
		}
	}

	public static class RegIndiviTableInputFormat extends TableInputFormat {
		// the whole region with relevant node is for one split

		// final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

		public static int staRelregnum = 0;
		public static long[] staRelreg = new long[200];
		public static double[][] nodeReg = new double[200][100];
		public static int[] nodeRegCnt = new int[200];
		public static int staTsplits = 0;

		public static long[] staRanReg = new long[150];
		public static int staRanRegCnt = 0;

		public static void getRegion(int num, long regid[],
				double nodePerReg[][], int nodePerRegCnt[]) {

			// System.out.printf("regions of interest: ");

			int tmp = 0;
			for (int i = 0; i < num; ++i) {
				staRelreg[i] = regid[i];

				nodeRegCnt[i] = nodePerRegCnt[i];
				tmp = nodeRegCnt[i];

				for (int k = 0; k < tmp; ++k) {
					nodeReg[i][k] = nodePerReg[i][k];
				}
				// public static int[] nodeRegCnt=new int[200];

			}
			staRelregnum = num;

			// System.out.printf("\n");
			return;
		}

		// public int splitsCal() throws IOException {
		// long[] selReg = new long[200];
		// int selRegCnt = 0;
		//
		// HTable table = this.getHTable(); // maybe super.getHTable
		// Scan scan = this.getScan(); // maybe super.getHTable
		//
		// if (table == null) {
		// throw new IOException("No table was provided.");
		// }
		// Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
		// if (keys == null || keys.getFirst() == null
		// || keys.getFirst().length == 0) {
		// throw new IOException("Expecting at least one region.");
		// }
		// int rescnt = 0, j = 0;
		//
		// String regionLocation;
		// HRegionInfo reginfor;
		// long tmpid = 0;
		//
		// for (int i = 0; i < keys.getFirst().length; i++) {
		// if (!includeRegionInSplit(keys.getFirst()[i],
		// keys.getSecond()[i])) {
		// continue;
		// }
		//
		// regionLocation = table.getRegionLocation(keys.getFirst()[i])
		// .getHostname();
		//
		// reginfor = table.getRegionLocation(keys.getFirst()[i])
		// .getRegionInfo();
		//
		// tmpid = reginfor.getRegionId();
		//
		// byte[] startRow = scan.getStartRow();
		// byte[] stopRow = scan.getStopRow();
		//
		// // .......................test...............................//
		// // System.out.printf("scan range for current region: %s %s\n",
		// // new String(scan.getStartRow()),
		// // new String(scan.getStopRow()));
		//
		// // determine if the given start an stop key fall into the region
		// if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
		// .compareTo(startRow, keys.getSecond()[i]) < 0)
		//
		// && (stopRow.length == 0 || Bytes.compareTo(stopRow,
		// keys.getFirst()[i]) > 0)) {
		//
		// rescnt++;
		//
		// // if (LOG.isDebugEnabled())
		// // LOG.debug("getSplits: split -> " + (count++) + " -> " +
		// // split);
		//
		// }
		//
		// // //eliminate current split
		// for (j = 0; j < staRelregnum; ++j) {
		// if (staRelreg[j] == tmpid) {
		// break;
		// }
		// }
		// if (j == staRelregnum)
		// continue;
		// else {
		// int k = 0;
		// for (k = 0; k < selRegCnt; ++k) {
		// if (selReg[k] == tmpid) {
		// break;
		// }
		// }
		// if (k == selRegCnt) {
		// selReg[selRegCnt++] = tmpid;
		// int tmpcnt = nodeRegCnt[j];
		//
		// for (int p = 0; p < tmpcnt; ++p) {
		//
		// rescnt++;
		// }
		// } else {
		// continue;
		// }
		// }
		//
		// }
		//
		// return rescnt;
		// }
		//
		// public int splitsInteCal() throws IOException {
		//
		// int j = 0;
		//
		// long[] selReg = new long[200];
		// int selRegCnt = 0;
		//
		// HTable table = this.getHTable(); // maybe super.getHTable
		// Scan scan = this.getScan(); // maybe super.getHTable
		//
		// if (table == null) {
		// throw new IOException("No table was provided.");
		// }
		// Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
		// if (keys == null || keys.getFirst() == null
		// || keys.getFirst().length == 0) {
		// throw new IOException("Expecting at least one region.");
		// }
		// int rescnt = 0;
		//
		// String regionLocation;
		// HRegionInfo reginfor;
		// long tmpid = 0;
		//
		// for (int i = 0; i < keys.getFirst().length; i++) {
		// if (!includeRegionInSplit(keys.getFirst()[i],
		// keys.getSecond()[i])) {
		// continue;
		// }
		//
		// regionLocation = table.getRegionLocation(keys.getFirst()[i])
		// .getHostname();
		//
		// reginfor = table.getRegionLocation(keys.getFirst()[i])
		// .getRegionInfo();
		//
		// tmpid = reginfor.getRegionId();
		//
		// byte[] startRow = scan.getStartRow();
		// byte[] stopRow = scan.getStopRow();
		//
		// // .......................test...............................//
		// // System.out.printf("scan range for current region: %s %s\n",
		// // new String(scan.getStartRow()),
		// // new String(scan.getStopRow()));
		//
		// // determine if the given start an stop key fall into the region
		// if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
		// .compareTo(startRow, keys.getSecond()[i]) < 0)
		//
		// && (stopRow.length == 0 || Bytes.compareTo(stopRow,
		// keys.getFirst()[i]) > 0)) {
		// // byte[] splitStart = startRow.length == 0
		// // || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
		// // keys
		// // .getFirst()[i] : startRow;
		// // byte[] splitStop = (stopRow.length == 0 ||
		// // Bytes.compareTo(
		// // keys.getSecond()[i], stopRow) <= 0)
		// // && keys.getSecond()[i].length > 0 ? keys
		// // .getSecond()[i] : stopRow;
		// rescnt++;
		// }
		// }
		// return rescnt;
		// }

		// @Override
		// public List<InputSplit> getSplits(JobContext context)
		// throws IOException {
		//
		// int j = 0;
		// // int tmpcnt = 0;
		// long[] selReg = new long[200];
		// int selRegCnt = 0;
		//
		// HTable table = this.getHTable(); // maybe super.getHTable
		// Scan scan = this.getScan(); // maybe super.getHTable
		//
		// if (table == null) {
		// throw new IOException("No table was provided.");
		// }
		// Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
		// if (keys == null || keys.getFirst() == null
		// || keys.getFirst().length == 0) {
		// throw new IOException("Expecting at least one region.");
		// }
		// // int count = 0;
		//
		// // String[] regserver = new String[20];
		// // int[] splitServer = new int[20];
		// // int regserverCnt = 0, tmpSplitCnt = 0, q = 0;
		//
		// List<InputSplit> splits = new ArrayList<InputSplit>(
		// keys.getFirst().length);
		//
		// String regionLocation;
		// HRegionInfo reginfor;
		// long tmpid = 0;
		//
		// for (int i = 0; i < keys.getFirst().length; i++) {
		// if (!includeRegionInSplit(keys.getFirst()[i],
		// keys.getSecond()[i])) {
		// continue;
		// }
		//
		// // tmpSplitCnt = 0;
		//
		// regionLocation = table.getRegionLocation(keys.getFirst()[i])
		// .getHostname(); // regionServer ID
		//
		// reginfor = table.getRegionLocation(keys.getFirst()[i])
		// .getRegionInfo();
		//
		// tmpid = reginfor.getRegionId();
		//
		// byte[] startRow = scan.getStartRow();
		// byte[] stopRow = scan.getStopRow();
		//
		// // ...........test...................//
		//
		// System.out.printf("..........new region: %s  %s....\n",
		// new String(keys.getFirst()[i]), new String(keys.getSecond()[i]));
		//
		// // ..................................//
		//
		// // .......................test...............................//
		// // System.out.printf("scan range for current region: %s %s\n",
		// // new String(scan.getStartRow()),
		// // new String(scan.getStopRow()));
		//
		// // determine if the given start an stop key fall into the region
		// if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
		// .compareTo(startRow, keys.getSecond()[i]) < 0)
		// && (stopRow.length == 0 || Bytes.compareTo(stopRow,
		// keys.getFirst()[i]) > 0)) {
		// byte[] splitStart = (startRow.length == 0 || Bytes
		// .compareTo(keys.getFirst()[i], startRow) >= 0) ? keys
		// .getFirst()[i] : startRow;
		// byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
		// keys.getSecond()[i], stopRow) <= 0)
		// && keys.getSecond()[i].length > 0 ? keys
		// .getSecond()[i] : stopRow;
		//
		// // byte[] splitStart = startRow.length == 0
		// // || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
		// // keys
		// // .getFirst()[i] : startRow;
		// // byte[] splitStop = (stopRow.length == 0 ||
		// // Bytes.compareTo(
		// // keys.getSecond()[i], stopRow) <= 0)
		// // && keys.getSecond()[i].length > 0 ? keys
		// // .getSecond()[i] : stopRow;
		//
		// // ........................test.......................//
		//
		// System.out.printf("new split: %s   %s  \n",
		// new String(splitStart), new String(splitStop));
		// // ...................................................//
		//
		// InputSplit split = new TableSplit(table.getTableName(),
		// splitStart, splitStop, regionLocation);
		//
		// // tmpSplitCnt++;
		// // if (LOG.isDebugEnabled())
		// // LOG.debug("getSplits: split -> " + (count++) + " -> " +
		// // split);
		//
		// // selReg[selRegCnt++] = tmpid;
		//
		// }
		//
		// // //eliminate current split
		// for (j = 0; j < staRelregnum; ++j) {
		// if (staRelreg[j] == tmpid) {
		// break;
		// }
		// }
		// if (j == staRelregnum)
		// continue;
		// else {
		// int k = 0;
		// for (k = 0; k < selRegCnt; ++k) {
		// if (selReg[k] == tmpid) {
		// break;
		// }
		// }
		// if (k == selRegCnt) {
		// selReg[selRegCnt++] = tmpid;
		// int tmpcnt = nodeRegCnt[j];
		//
		// for (int p = 0; p < tmpcnt; ++p) {
		//
		// // .......test.........................//
		//
		// String tmp1 = regvalKeyCon((long) nodeReg[j][p])
		// + "," + "0";
		// String tmp2 = regvalKeyCon((long) (nodeReg[j][p] + 1))
		// + "," + "0";
		//
		// System.out
		// .printf("new split %s   %s\n", tmp1, tmp2);
		//
		// // ....................................//
		//
		// InputSplit split = new TableSplit(
		// table.getTableName(),
		// Bytes.toBytes(regvalKeyCon((long) nodeReg[j][p])
		// + "," + "0"),
		// Bytes.toBytes(regvalKeyCon((long) (nodeReg[j][p] + 1))
		// + "," + "0"), regionLocation);
		// splits.add(split);
		//
		// // tmpSplitCnt++;
		//
		// }
		// } else {
		// continue;
		// }
		// }
		// // }
		//
		// // ...........test...................//
		//
		// // if (tmpSplitCnt == 0) {
		// // System.out.printf("  \n");
		// // } else {
		// // System.out.printf("overlapping region: %d  \n", tmpSplitCnt);
		// // }
		// // ..................................//
		//
		// // for (q = 0; q < regserverCnt; ++q) {
		// // if (regionLocation == regserver[q]) {
		// // splitServer[q] += tmpSplitCnt;
		// // break;
		// // }
		// // }
		// // if (q == regserverCnt) {
		// // regserver[regserverCnt] = regionLocation;
		// // splitServer[regserverCnt++] = tmpSplitCnt;
		// //
		// // }
		//
		// // regionLocation
		// //
		// // String [] regserver= new String[20];
		// // int[] splitServer = new int[20];
		// // int regserverCnt=0,tmpSplitCnt=0;
		// }
		//
		// // ........test...................//
		// // System.out.printf("the number of splits: %d\n", splits.size());
		// //
		// // System.out
		// // .printf("the split distribution accross the region server:");
		// //
		// // for (q = 0; q < regserverCnt; ++q) {
		// // System.out.printf("%s %d, ", regserver[q], splitServer[q]);
		// // }
		// // System.out.printf("\n");
		// // ...............................//
		//
		// return splits;
		//
		// // List<InputSplit> splits = new ArrayList<InputSplit>();
		// // Scan scan = getScan();
		// // byte startRow[] = scan.getStartRow(), stopRow[] =
		// // scan.getStopRow();
		// // byte prefixedStartRow[] = new byte[startRow.length + 1];
		// // byte prefixedStopRow[] = new byte[stopRow.length + 1];
		// // System.arraycopy(startRow, 0, prefixedStartRow, 1,
		// // startRow.length);
		// // System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
		// //
		// // for (int prefix = -128; prefix < 128; prefix++) {
		// // prefixedStartRow[0] = (byte) prefix;
		// // prefixedStopRow[0] = (byte) prefix;
		// // scan.setStartRow(prefixedStartRow);
		// // scan.setStopRow(prefixedStopRow);
		// // setScan(scan);
		// // splits.addAll(super.getSplits(context));
		// // }
		//
		// // return splits;
		// }

		public int calSplits() throws IOException {

			int j = 0;
			long[] selReg = new long[200];
			int selRegCnt = 0;

			HTable table = this.getHTable(); // maybe super.getHTable
			Scan scan = this.getScan(); // maybe super.getHTable

			if (table == null) {
				throw new IOException("No table was provided.");
			}
			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}

			List<InputSplit> splits = new ArrayList<InputSplit>(
					keys.getFirst().length);

			String regionLocation;
			HRegionInfo reginfor;
			long tmpid = 0;

			String[] regserver = new String[20];
			int[] splitServer = new int[20];
			int regserverCnt = 0, tmpSplitCnt = 0, g = 0;

			int lflag = 0, rflag = 0;

			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}

				tmpSplitCnt = 0;

				regionLocation = table.getRegionLocation(keys.getFirst()[i])
						.getHostname();

				reginfor = table.getRegionLocation(keys.getFirst()[i])
						.getRegionInfo();

				tmpid = reginfor.getRegionId();

				byte[] startRow = scan.getStartRow();
				byte[] stopRow = scan.getStopRow();

				// determine if the given start an stop key fall into the region

				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
						.compareTo(startRow, keys.getSecond()[i]) < 0)

						&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
								keys.getFirst()[i]) > 0)) {

					if (startRow.length == 0
							|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0) {
						byte[] splitStart = keys.getFirst()[i];
						lflag = 1;
					} else {
						lflag = 0;
						byte[] splitStart = startRow;
					}

					if ((stopRow.length == 0 || Bytes.compareTo(
							keys.getSecond()[i], stopRow) <= 0)
							&& keys.getSecond()[i].length > 0) {
						byte[] splitStop = keys.getSecond()[i];
						rflag = 1;
					} else {
						rflag = 0;
						byte[] splitStop = stopRow;
					}

					tmpSplitCnt++;

					staRanReg[staRanRegCnt++] = tmpid;

					// if (LOG.isDebugEnabled())
					// LOG.debug("getSplits: split -> " + (count++) + " -> " +
					// split);

					// selReg[selRegCnt++] = tmpid;

				}

				// //eliminate current split
				for (j = 0; j < staRelregnum; ++j) {
					if (staRelreg[j] == tmpid) {
						break;
					}
				}
				if (j == staRelregnum) {
					// continue;
				} else {
					int k = 0;
					for (k = 0; k < selRegCnt; ++k) {
						if (selReg[k] == tmpid) {
							break;
						}
					}
					if (k == selRegCnt) {
						selReg[selRegCnt++] = tmpid;
						int tmpcnt = nodeRegCnt[j];

						for (int p = 0; p < tmpcnt; ++p) {
							tmpSplitCnt++;
						}
					} else {
						// continue;
					}
				}
			}
			return tmpSplitCnt;

		}

		public List<InputSplit> getSplits(JobContext context)
				throws IOException {

			int j = 0;
			// int tmpcnt = 0;
			long[] selReg = new long[200];
			int selRegCnt = 0;

			HTable table = this.getHTable(); // maybe super.getHTable
			Scan scan = this.getScan(); // maybe super.getHTable

			if (table == null) {
				throw new IOException("No table was provided.");
			}
			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}
			// int count = 0;

			List<InputSplit> splits = new ArrayList<InputSplit>(
					keys.getFirst().length);

			String regionLocation;
			HRegionInfo reginfor;
			long tmpid = 0;

			String[] regserver = new String[20];
			int[] splitServer = new int[20];
			int regserverCnt = 0, tmpSplitCnt = 0, g = 0;

			staTsplits = 0;
			staRanRegCnt = 0;

			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}

				tmpSplitCnt = 0;

				regionLocation = table.getRegionLocation(keys.getFirst()[i])
						.getHostname();

				reginfor = table.getRegionLocation(keys.getFirst()[i])
						.getRegionInfo();

				tmpid = reginfor.getRegionId();

				byte[] startRow = scan.getStartRow();
				byte[] stopRow = scan.getStopRow();

				// .......................test...............................//
				// System.out.printf("scan range for current region: %s %s\n",
				// new String(scan.getStartRow()),
				// new String(scan.getStopRow()));

				// determine if the given start an stop key fall into the region
				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
						.compareTo(startRow, keys.getSecond()[i]) < 0)

						&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
								keys.getFirst()[i]) > 0)) {
					byte[] splitStart = startRow.length == 0
							|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
							.getFirst()[i] : startRow;
					byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
							keys.getSecond()[i], stopRow) <= 0)
							&& keys.getSecond()[i].length > 0 ? keys
							.getSecond()[i] : stopRow;

					// .............test........................//
					System.out.printf("new split: %s %s\n", new String(
							splitStart), new String(splitStop));
					// ..........................................//

					InputSplit split = new TableSplit(table.getTableName(),
							splitStart, splitStop, regionLocation);
					splits.add(split);
					staTsplits++;

					staRanReg[staRanRegCnt++] = tmpid;
					// if (LOG.isDebugEnabled())
					// LOG.debug("getSplits: split -> " + (count++) + " -> " +
					// split);

					// selReg[selRegCnt++] = tmpid;

				}

				// //eliminate current split
				for (j = 0; j < staRelregnum; ++j) {
					if (staRelreg[j] == tmpid) {
						break;
					}
				}
				if (j == staRelregnum) {
					// continue;
				} else {
					int k = 0;
					for (k = 0; k < selRegCnt; ++k) {
						if (selReg[k] == tmpid) {
							break;
						}
					}
					if (k == selRegCnt) {
						selReg[selRegCnt++] = tmpid;
						int tmpcnt = nodeRegCnt[j];

						for (int p = 0; p < tmpcnt; ++p) {

							byte[] tmpl = Bytes
									.toBytes(regvalKeyCon((long) nodeReg[j][p])
											+ "," + "0");
							byte[] tmpr = Bytes
									.toBytes(regvalKeyCon((long) nodeReg[j][p] + 1)
											+ "," + "0");

							byte[] splitStart;
							byte[] splitStop;

							if (Bytes.compareTo(keys.getFirst()[i], tmpl) <= 0) {
								splitStart = tmpl;
							} else {
								splitStart = keys.getFirst()[i];
							}
							if (Bytes.compareTo(keys.getSecond()[i], tmpr) > 0) {
								splitStop = tmpr;
							} else {
								splitStop = keys.getSecond()[i];
							}

							InputSplit split = new TableSplit(
									table.getTableName(), splitStart,
									splitStop, regionLocation);
							splits.add(split);
							staTsplits++;

						}
					} else {
						// continue;
					}
				}

				// String[] regserver = new String[20];
				// int[] splitServer = new int[20];
				// int regserverCnt = 0, tmpSplitCnt = 0, g = 0;
				//
				// for (g = 0; g < regserverCnt; ++g) {
				// if (regionLocation.compareTo(regserver[g]) == 0) {
				// splitServer[g] += tmpSplitCnt;
				// break;
				// }
				// }
				// if (g == regserverCnt) {
				// regserver[g] = regionLocation;
				// splitServer[g] = tmpSplitCnt;
				// regserverCnt++;
				// }

			}

			// ........test...................//
			// System.out.printf("the number of splits: %d\n", splits.size());
			//
			// System.out.printf("split distribution across regionserver\n");
			// for (int i = 0; i < regserverCnt; ++i) {
			// System.out.printf("region server %s: %d\n", regserver[i],
			// splitServer[i]);
			//
			// }
			// System.out.printf("\n");
			// ...............................//

			// .............test........................//
			System.out.printf("total split number: %d \n", staTsplits);
			// ..........................................//

			// ? staTsplits=tmpSplitCnt;
			return splits;

			// List<InputSplit> splits = new ArrayList<InputSplit>();
			// Scan scan = getScan();
			// byte startRow[] = scan.getStartRow(), stopRow[] =
			// scan.getStopRow();
			// byte prefixedStartRow[] = new byte[startRow.length + 1];
			// byte prefixedStopRow[] = new byte[stopRow.length + 1];
			// System.arraycopy(startRow, 0, prefixedStartRow, 1,
			// startRow.length);
			// System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
			//
			// for (int prefix = -128; prefix < 128; prefix++) {
			// prefixedStartRow[0] = (byte) prefix;
			// prefixedStopRow[0] = (byte) prefix;
			// scan.setStartRow(prefixedStartRow);
			// scan.setStopRow(prefixedStopRow);
			// setScan(scan);
			// splits.addAll(super.getSplits(context));
			// }

			// return splits;
		}
	}

	public static class RegAdaTableInputFormat extends TableInputFormat {
		// the whole region with relevant node is for one split

		// final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

		public static int staAdaSegnum = 0;
		public static double[][] staAdaSeg = new double[200][3];

		public static void getRegion(int num, double nodeSeg[][]) {

			for (int i = 0; i < num; ++i) {
				for (int j = 0; j < 3; ++j) {
					staAdaSeg[i][j] = nodeSeg[i][j];
				}
			}
			staAdaSegnum = num;
			return;
		}

		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException {

			int j = 0;
			long[] selReg = new long[200];
			int selRegCnt = 0;

			HTable table = this.getHTable(); // maybe super.getHTable
			Scan scan = this.getScan(); // maybe super.getHTable

			if (table == null) {
				throw new IOException("No table was provided.");
			}
			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}

			List<InputSplit> splits = new ArrayList<InputSplit>(
					keys.getFirst().length);

			String regionLocation;
			HRegionInfo reginfor;
			long tmpid = 0;

			int segCnt = 0;
			String curSegl = "", curSegr = "", regl = "", regr = "";
			curSegl = regvalKeyCon((long) staAdaSeg[segCnt][0]);
			curSegr = regvalKeyCon((long) staAdaSeg[segCnt][1]);

			int flag = 0;

			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}

				regionLocation = table.getRegionLocation(keys.getFirst()[i])
						.getHostname();
				reginfor = table.getRegionLocation(keys.getFirst()[i])
						.getRegionInfo();
				tmpid = reginfor.getRegionId();

				byte[] startRow = scan.getStartRow();
				byte[] stopRow = scan.getStopRow();

				regl = new String(keys.getFirst()[i]);
				regr = new String(keys.getSecond()[i]);

				if (flag == 0) {
					while (curSegl.compareTo(regl) >= 0
							&& curSegl.compareTo(regr) <= 0) {

						if (curSegr.compareTo(regl) >= 0
								&& curSegr.compareTo(regr) <= 0) {

							byte[] splitStart = Bytes.toBytes(curSegl);
							byte[] splitStop = Bytes.toBytes(curSegr);

							InputSplit split = new TableSplit(
									table.getTableName(), splitStart,
									splitStop, regionLocation);
							splits.add(split);

							segCnt++;
							curSegl = regvalKeyCon((long) staAdaSeg[segCnt][0]);
							curSegr = regvalKeyCon((long) staAdaSeg[segCnt][1]);
						} else {
							flag = 1;
							break;
						}
					}
					// flag=0;
					continue;
				} else {
					if (curSegr.compareTo(regl) >= 0
							&& curSegr.compareTo(regr) <= 0) {

						byte[] splitStart = Bytes.toBytes(curSegl);
						byte[] splitStop = Bytes.toBytes(curSegr);

						InputSplit split = new TableSplit(table.getTableName(),
								splitStart, splitStop, regionLocation);
						splits.add(split);

						segCnt++;
						curSegl = regvalKeyCon((long) staAdaSeg[segCnt][0]);
						curSegr = regvalKeyCon((long) staAdaSeg[segCnt][1]);

						flag = 0;

						while (curSegl.compareTo(regl) >= 0
								&& curSegl.compareTo(regr) <= 0) {

							if (curSegr.compareTo(regl) >= 0
									&& curSegr.compareTo(regr) <= 0) {

								splitStart = Bytes.toBytes(curSegl);
								splitStop = Bytes.toBytes(curSegr);

								split = new TableSplit(table.getTableName(),
										splitStart, splitStop, regionLocation);
								splits.add(split);

								segCnt++;
								curSegl = regvalKeyCon((long) staAdaSeg[segCnt][0]);
								curSegr = regvalKeyCon((long) staAdaSeg[segCnt][1]);
							} else {
								flag = 1;
								break;
							}
						}
						flag = 0;
					} else {
						continue;
					}
				}

				// determine if the given start an stop key fall into the region
				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
						.compareTo(startRow, keys.getSecond()[i]) < 0)

						&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
								keys.getFirst()[i]) > 0)) {
					byte[] splitStart = startRow.length == 0
							|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
							.getFirst()[i] : startRow;
					byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
							keys.getSecond()[i], stopRow) <= 0)
							&& keys.getSecond()[i].length > 0 ? keys
							.getSecond()[i] : stopRow;

					InputSplit split = new TableSplit(table.getTableName(),
							splitStart, splitStop, regionLocation);
					splits.add(split);
					// if (LOG.isDebugEnabled())
					// LOG.debug("getSplits: split -> " + (count++) + " -> " +
					// split);

					// selReg[selRegCnt++] = tmpid;

				}

				// //eliminate current split

				// }
			}

			// ........test...................//
			System.out.printf("the number of splits: %d\n", splits.size());

			// ...............................//

			return splits;

			// List<InputSplit> splits = new ArrayList<InputSplit>();
			// Scan scan = getScan();
			// byte startRow[] = scan.getStartRow(), stopRow[] =
			// scan.getStopRow();
			// byte prefixedStartRow[] = new byte[startRow.length + 1];
			// byte prefixedStopRow[] = new byte[stopRow.length + 1];
			// System.arraycopy(startRow, 0, prefixedStartRow, 1,
			// startRow.length);
			// System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
			//
			// for (int prefix = -128; prefix < 128; prefix++) {
			// prefixedStartRow[0] = (byte) prefix;
			// prefixedStopRow[0] = (byte) prefix;
			// scan.setStartRow(prefixedStartRow);
			// scan.setStopRow(prefixedStopRow);
			// setScan(scan);
			// splits.addAll(super.getSplits(context));
			// }

			// return splits;
		}
	}

	// public static class RegBaselineAdaTableInputFormat extends
	// TableInputFormat {
	// // the whole region with relevant node is for one split
	//
	// // final Log LOG = LogFactory.getLog(TableInputFormatBase.class);
	//
	// // public static int staAdaSegnum = 0;
	// // public static double[][] staAdaSeg = new double[200][3];
	// //
	// // public static void getRegion(int num, double nodeSeg[][]) {
	// //
	// // for (int i = 0; i < num; ++i) {
	// // for (int j = 0; j < 3; ++j) {
	// // staAdaSeg[i][j] = nodeSeg[i][j];
	// // }
	// // }
	// // staAdaSegnum = num;
	// // return;
	// // }
	//
	//
	//
	//
	// public static int staRelnodeCnt = 0;
	// public static double[] staRelnode = new double[200];
	//
	// public static int mapcons=0;
	// public static int cursplit=0;
	// // public static double[][] nodeReg = new double[200][100];
	// // public static int[] nodeRegCnt = new int[200];
	//
	// public static void getRegion(int cnt, double extnode[],int cons, int
	// splits) {
	//
	// for (int i = 0; i < cnt; ++i) {
	// staRelnode[i]=extnode[i];
	// }
	// staRelnodeCnt = cnt;
	//
	// mapcons=cons;
	// cursplit=splits;
	// return;
	// }
	//
	// public int splitsInteCal() throws IOException
	// {
	//
	// int j = 0;
	//
	// long[] selReg = new long[200];
	// int selRegCnt = 0;
	//
	// HTable table = this.getHTable(); // maybe super.getHTable
	// Scan scan = this.getScan(); // maybe super.getHTable
	//
	// if (table == null) {
	// throw new IOException("No table was provided.");
	// }
	// Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
	// if (keys == null || keys.getFirst() == null
	// || keys.getFirst().length == 0) {
	// throw new IOException("Expecting at least one region.");
	// }
	// int rescnt=0;
	//
	// String regionLocation;
	// HRegionInfo reginfor;
	// long tmpid = 0;
	//
	// for (int i = 0; i < keys.getFirst().length; i++) {
	// if (!includeRegionInSplit(keys.getFirst()[i],
	// keys.getSecond()[i])) {
	// continue;
	// }
	//
	// regionLocation = table.getRegionLocation(keys.getFirst()[i])
	// .getHostname();
	//
	// reginfor = table.getRegionLocation(keys.getFirst()[i])
	// .getRegionInfo();
	//
	// tmpid = reginfor.getRegionId();
	//
	// byte[] startRow = scan.getStartRow();
	// byte[] stopRow = scan.getStopRow();
	//
	// // .......................test...............................//
	// // System.out.printf("scan range for current region: %s %s\n",
	// // new String(scan.getStartRow()),
	// // new String(scan.getStopRow()));
	//
	// // determine if the given start an stop key fall into the region
	// if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
	// .compareTo(startRow, keys.getSecond()[i]) < 0)
	//
	// && (stopRow.length == 0 || Bytes.compareTo(stopRow,
	// keys.getFirst()[i]) > 0)) {
	// // byte[] splitStart = startRow.length == 0
	// // || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
	// // .getFirst()[i] : startRow;
	// // byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
	// // keys.getSecond()[i], stopRow) <= 0)
	// // && keys.getSecond()[i].length > 0 ? keys
	// // .getSecond()[i] : stopRow;
	// rescnt++;
	// }
	// }
	// return rescnt;
	// }
	//
	//
	// @Override
	// public List<InputSplit> getSplits(JobContext context)
	// throws IOException {
	//
	// int j = 0;
	// long[] selReg = new long[200];
	// int selRegCnt = 0;
	//
	// HTable table = this.getHTable(); // maybe super.getHTable
	// Scan scan = this.getScan(); // maybe super.getHTable
	//
	// if (table == null) {
	// throw new IOException("No table was provided.");
	// }
	// Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
	// if (keys == null || keys.getFirst() == null
	// || keys.getFirst().length == 0) {
	// throw new IOException("Expecting at least one region.");
	// }
	//
	// List<InputSplit> splits = new ArrayList<InputSplit>(
	// keys.getFirst().length);
	//
	//
	// // mapcons=cons;
	// // cursplit=splits;
	//
	// double groupk=2;
	// while((cursplit/groupk)>mapcons)
	// {
	// groupk++;
	// }
	//
	// staRelnode
	//
	//
	// Bytes.toBytes(regvalKeyCon((long) nodeReg[j][p])
	// + "," + "0"),
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	// String regionLocation;
	// HRegionInfo reginfor;
	// long tmpid = 0;
	//
	// int segCnt = 0;
	// String curSegl = "", curSegr = "", regl = "", regr = "";
	// curSegl = regvalKeyCon((long) staAdaSeg[segCnt][0]);
	// curSegr = regvalKeyCon((long) staAdaSeg[segCnt][1]);
	//
	// int flag = 0;
	//
	// for (int i = 0; i < keys.getFirst().length; i++) {
	// if (!includeRegionInSplit(keys.getFirst()[i],
	// keys.getSecond()[i])) {
	// continue;
	// }
	//
	// regionLocation = table.getRegionLocation(keys.getFirst()[i])
	// .getHostname();
	// reginfor = table.getRegionLocation(keys.getFirst()[i])
	// .getRegionInfo();
	// tmpid = reginfor.getRegionId();
	//
	// byte[] startRow = scan.getStartRow();
	// byte[] stopRow = scan.getStopRow();
	//
	// regl = new String(keys.getFirst()[i]);
	// regr = new String(keys.getSecond()[i]);
	//
	//
	// // determine if the given start an stop key fall into the region
	// if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
	// .compareTo(startRow, keys.getSecond()[i]) < 0)
	//
	// && (stopRow.length == 0 || Bytes.compareTo(stopRow,
	// keys.getFirst()[i]) > 0)) {
	// byte[] splitStart = startRow.length == 0
	// || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
	// .getFirst()[i] : startRow;
	// byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
	// keys.getSecond()[i], stopRow) <= 0)
	// && keys.getSecond()[i].length > 0 ? keys
	// .getSecond()[i] : stopRow;
	//
	// InputSplit split = new TableSplit(table.getTableName(),
	// splitStart, splitStop, regionLocation);
	// splits.add(split);
	// // if (LOG.isDebugEnabled())
	// // LOG.debug("getSplits: split -> " + (count++) + " -> " +
	// // split);
	//
	// // selReg[selRegCnt++] = tmpid;
	//
	// }
	//
	//
	// }
	//
	// // ........test...................//
	// System.out.printf("the number of splits: %d\n", splits.size());
	//
	// // ...............................//
	//
	// return splits;
	//
	// // List<InputSplit> splits = new ArrayList<InputSplit>();
	// // Scan scan = getScan();
	// // byte startRow[] = scan.getStartRow(), stopRow[] =
	// // scan.getStopRow();
	// // byte prefixedStartRow[] = new byte[startRow.length + 1];
	// // byte prefixedStopRow[] = new byte[stopRow.length + 1];
	// // System.arraycopy(startRow, 0, prefixedStartRow, 1,
	// // startRow.length);
	// // System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
	// //
	// // for (int prefix = -128; prefix < 128; prefix++) {
	// // prefixedStartRow[0] = (byte) prefix;
	// // prefixedStopRow[0] = (byte) prefix;
	// // scan.setStartRow(prefixedStartRow);
	// // scan.setStopRow(prefixedStopRow);
	// // setScan(scan);
	// // splits.addAll(super.getSplits(context));
	// // }
	//
	// // return splits;
	// }
	// }

	static class MapperTempInterval extends
			TableMapper<ImmutableBytesWritable, Text> {

		public static long cntnum = 0;
		public Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		public String modelContract(Result values, int order, double lb,
				double rb) {
			String res = "";
			res = Double.toString(lb) + ",";
			res += Double.toString(rb) + ",";

			// String strtmp = "";
			for (int i = 0; i < order + 1; i++) {

				res = res
						+ Bytes.toString(values.getValue(
								Bytes.toBytes("model"),
								Bytes.toBytes("coef" + Integer.toString(i))));
				// tmp=Double.parseDouble(strtmp);
				// strtmp=Double.toString(tmp);

				// res = res + strtmp;
			}

			return res;
		}

		public String modelContract(Result values, int order, String bdstr) {
			String res = new String();
			res = bdstr + ",";
			// res = Double.toString(lb) + ",";
			// res += Double.toString(rb) + ",";

			// String strtmp = "";
			for (int i = 0; i < order + 1; i++) {

				res = res
						+ Bytes.toString(values.getValue(
								Bytes.toBytes("model"),
								Bytes.toBytes("coef" + Integer.toString(i))));

				// res = res + strtmp;
			}

			return res;
		}

		public void attriCfParser(String range, double interv[]) {
			int len = range.length();
			int st = 0;
			int num = 0;
			for (int i = 0; i < len; ++i) {
				if (range.charAt(i) == ',') {
					interv[num++] = Double.parseDouble(range.substring(st, i));
					st = i + 1;
				}
			}

			return;
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			try {
				Configuration conf = context.getConfiguration();
				lbstr = conf.get("lb");
				rbstr = conf.get("rb");
				modOrdstr = conf.get("modelOrd");

				lb = Double.parseDouble(lbstr);
				rb = Double.parseDouble(rbstr);
				modOrd = Integer.parseInt(modOrdstr);

				lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("st")));
				rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("ed")));

				bd[0] = Double.parseDouble(lbstr);
				bd[1] = Double.parseDouble(rbstr);

				// ...................//

				model.set(modelContract(values, modOrd, bd[0], bd[1]));
				numMapIn++;
				cntnum++;
				// model.set(modelContract(values, modOrd, interv));

				if (lb > bd[1] || rb < bd[0]) // no overlap
				{

				} else {

					intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	static class MapperValInterval extends
			TableMapper<ImmutableBytesWritable, Text> {

		private long cntnum = 0;
		private Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		public String modelContract(Result values, int order, double lb,
				double rb) {
			String res = "";
			res = Double.toString(lb) + ";";
			res += Double.toString(rb) + ";";

			res = res
					+ Bytes.toString(values.getValue(Bytes.toBytes("attri"),
							Bytes.toBytes("st"))) + ";";
			res = res
					+ Bytes.toString(values.getValue(Bytes.toBytes("attri"),
							Bytes.toBytes("ed"))) + ";";

			for (int i = 0; i < order + 1; i++) {

				res = res
						+ Bytes.toString(values.getValue(
								Bytes.toBytes("model"),
								Bytes.toBytes("coef" + Integer.toString(i))));
			}

			return res;
		}

		// public String modelContract(Result values, int order, String bdstr) {
		// String res = new String();
		// res = bdstr + ",";
		// // res = Double.toString(lb) + ",";
		// // res += Double.toString(rb) + ",";
		//
		// // String strtmp = "";
		// for (int i = 0; i < order + 1; i++) {
		//
		// res = res
		// + Bytes.toString(values.getValue(
		// Bytes.toBytes("model"),
		// Bytes.toBytes("coef" + Integer.toString(i))));
		//
		// // res = res + strtmp;
		// }
		//
		// return res;
		// }

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			try {
				Configuration conf = context.getConfiguration();
				lbstr = conf.get("lb");
				rbstr = conf.get("rb");
				modOrdstr = conf.get("modelOrd");

				lb = Double.parseDouble(lbstr);
				rb = Double.parseDouble(rbstr);
				modOrd = Integer.parseInt(modOrdstr);

				lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vl")));
				rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vr")));

				bd[0] = Double.parseDouble(lbstr);
				bd[1] = Double.parseDouble(rbstr);

				// ...................//

				model.set(modelContract(values, modOrd, bd[0], bd[1]));
				numMapIn++;
				cntnum++;
				// model.set(modelContract(values, modOrd, interv));

				if (lb > bd[1] || rb < bd[0]) // no overlap
				{

				} else {

					intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	static class MapperComposInterval extends
			TableMapper<ImmutableBytesWritable, Text> {

		public static long cntnum = 0;
		public Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String tlbstr = new String();
		String trbstr = new String();
		String vlbstr = new String();
		String vrbstr = new String();
		String modOrdstr = new String();

		double tlb = 0.0, trb = 0.0, vlb = 0.0, vrb = 0.0;
		int modOrd = 0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		public String modelContract(Result values, int order, double lb,
				double rb) {
			String res = "";
			res = Double.toString(lb) + ",";
			res += Double.toString(rb) + ",";

			// String strtmp = "";
			for (int i = 0; i < order + 1; i++) {

				res = res
						+ Bytes.toString(values.getValue(
								Bytes.toBytes("model"),
								Bytes.toBytes("coef" + Integer.toString(i))));
				// tmp=Double.parseDouble(strtmp);
				// strtmp=Double.toString(tmp);

				// res = res + strtmp;
			}

			return res;
		}

		public String modelContract(Result values, int order, String bdstr) {
			String res = new String();
			res = bdstr + ",";
			// res = Double.toString(lb) + ",";
			// res += Double.toString(rb) + ",";

			// String strtmp = "";
			for (int i = 0; i < order + 1; i++) {

				res = res
						+ Bytes.toString(values.getValue(
								Bytes.toBytes("model"),
								Bytes.toBytes("coef" + Integer.toString(i))));

				// res = res + strtmp;
			}

			return res;
		}

		public void attriCfParser(String range, double interv[]) {
			int len = range.length();
			int st = 0;
			int num = 0;
			for (int i = 0; i < len; ++i) {
				if (range.charAt(i) == ',') {
					interv[num++] = Double.parseDouble(range.substring(st, i));
					st = i + 1;
				}
			}

			return;
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			try {
				Configuration conf = context.getConfiguration();
				tlbstr = conf.get("tlb");
				trbstr = conf.get("trb");

				vlbstr = conf.get("vlb");
				vrbstr = conf.get("vrb");

				modOrdstr = conf.get("modelOrd");

				tlb = Double.parseDouble(tlbstr);
				trb = Double.parseDouble(trbstr);
				vlb = Double.parseDouble(vlbstr);
				vrb = Double.parseDouble(vrbstr);
				modOrd = Integer.parseInt(modOrdstr);

				tlbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("st")));
				trbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("ed")));

				vlbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vl")));
				vrbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vr")));

				bd[0] = Double.parseDouble(tlbstr);
				bd[1] = Double.parseDouble(trbstr);

				bd[2] = Double.parseDouble(vlbstr);
				bd[3] = Double.parseDouble(vrbstr);

				// ...................//

				model.set(modelContract(values, modOrd, bd[0], bd[1]));
				numMapIn++;
				cntnum++;
				// model.set(modelContract(values, modOrd, interv));

				if (tlb > bd[1] || trb < bd[0]) // no overlap
				{

				} else {

					if (vlb > bd[1] || vrb < bd[0]) {

					} else {
						intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
						context.write(intkey, model);
					}
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	static class MapperTempIntervalNoRed extends TableMapper<Text, Text> {

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			Configuration conf = context.getConfiguration();
			lbstr = conf.get("lb");
			rbstr = conf.get("rb");
			modOrdstr = conf.get("modelOrd");

			lb = Double.parseDouble(lbstr);
			rb = Double.parseDouble(rbstr);
			modOrd = Integer.parseInt(modOrdstr);

			lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
					Bytes.toBytes("st")));
			rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
					Bytes.toBytes("ed")));

			bd[0] = Double.parseDouble(lbstr);
			bd[1] = Double.parseDouble(rbstr);

			// ...................//

			// model.set(modelContract(values, modOrd, bd[0], bd[1]));
			// numMapIn++;
			// model.set(modelContract(values, modOrd, interv));

			if (lb > bd[1] || rb < bd[0]) // no overlap
			{

			} else {

				// intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
				// context.write(intkey, model);
			}
		}
	}

	static class MapperValIntervalNoRed extends TableMapper<Text, Text> {

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			Configuration conf = context.getConfiguration();
			lbstr = conf.get("lb");
			rbstr = conf.get("rb");
			modOrdstr = conf.get("modelOrd");

			lb = Double.parseDouble(lbstr);
			rb = Double.parseDouble(rbstr);
			modOrd = Integer.parseInt(modOrdstr);

			lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
					Bytes.toBytes("vl")));
			rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
					Bytes.toBytes("vr")));

			bd[0] = Double.parseDouble(lbstr);
			bd[1] = Double.parseDouble(rbstr);

			// ...................//

			// model.set(modelContract(values, modOrd, bd[0], bd[1]));
			// numMapIn++;
			// model.set(modelContract(values, modOrd, interv));

			if (lb > bd[1] || rb < bd[0]) // no overlap
			{

			} else {

				// intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
				// context.write(intkey, model);
			}
		}
	}

	static class MapperTempPoint extends
			TableMapper<ImmutableBytesWritable, Text> {

		public static long cntnum = 0;
		public Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		double val = 0.0;

		String valstr = new String();
		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		public String modelContract(Result values, int order, double lb,
				double rb) {
			String res = "";
			res = Double.toString(lb) + ",";
			res += Double.toString(rb) + ",";

			// String strtmp = "";
			for (int i = 0; i < order + 1; i++) {

				res = res
						+ Bytes.toString(values.getValue(
								Bytes.toBytes("model"),
								Bytes.toBytes("coef" + Integer.toString(i))));
				// tmp=Double.parseDouble(strtmp);
				// strtmp=Double.toString(tmp);

				// res = res + strtmp;
			}

			return res;
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			try {
				Configuration conf = context.getConfiguration();
				valstr = conf.get("val");
				modOrdstr = conf.get("modelOrd");
				modOrd = Integer.parseInt(modOrdstr);

				lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("st")));
				rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("ed")));

				val = Double.parseDouble(valstr);
				bd[0] = Double.parseDouble(lbstr);
				bd[1] = Double.parseDouble(rbstr);

				// ...................//

				model.set(modelContract(values, modOrd, bd[0], bd[1]));
				numMapIn++;
				cntnum++;

				if (val <= bd[1] && val >= bd[0]) {
					intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	static class MapperValPoint extends
			TableMapper<ImmutableBytesWritable, Text> {

		public static long cntnum = 0;
		public Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		double val = 0.0;

		String valstr = new String();
		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		public String modelContract(Result values, int order, double lb,
				double rb) {
			String res = "";
			res = Double.toString(lb) + ",";
			res += Double.toString(rb) + ",";

			// String strtmp = "";
			for (int i = 0; i < order + 1; i++) {

				res = res
						+ Bytes.toString(values.getValue(
								Bytes.toBytes("model"),
								Bytes.toBytes("coef" + Integer.toString(i))));
				// tmp=Double.parseDouble(strtmp);
				// strtmp=Double.toString(tmp);

				// res = res + strtmp;
			}

			return res;
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			try {
				Configuration conf = context.getConfiguration();
				valstr = conf.get("val");
				modOrdstr = conf.get("modelOrd");
				modOrd = Integer.parseInt(modOrdstr);

				lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vl")));
				rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vr")));

				val = Double.parseDouble(valstr);
				bd[0] = Double.parseDouble(lbstr);
				bd[1] = Double.parseDouble(rbstr);

				// ...................//

				model.set(modelContract(values, modOrd, bd[0], bd[1]));
				numMapIn++;
				cntnum++;

				if (val <= bd[1] && val >= bd[0]) {
					intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	public static class ReducerInterTemp extends // for interval query
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

		public double cntnum = 0;
		public double[] coef = new double[10], range = new double[5];
		public double[] res = new double[100];
		public double respoint = 0.0;

		public double step = 0.0;
		public double st = 0.0, ed = 0.0;
		String reskey = new String();

		public double gridstep = 5;// 30.0;

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		Put put;
		

		byte[] CF = "attri".getBytes();
		byte[] VAL = "val".getBytes();
		
		

		public void modelParser2(String modelstr, double coef[],
				double range[], String secRange[])// range[0]:
		// left time, right time, left value, right value
		{

			int len = modelstr.length(), st = 0, num = 0;
			for (int i = 0; i < len; ++i) {
				if (modelstr.charAt(i) == ';') {

					if (num < 2) {
						range[num++] = Double.parseDouble(modelstr.substring(
								st, i));
						st = i + 1;
					} else if (num >= 2) {
						secRange[num - 2] = modelstr.substring(st, i);
						num++;
						st = i + 1;
						break;
					} else if (num >= 4) {
						st = i + 1;
						break;
					}
				}
			}

			coef[0] = Double.parseDouble(modelstr.substring(st, len));

			return;
		}

		// @Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			lbstr = conf.get("lb");
			rbstr = conf.get("rb");
			modOrdstr = conf.get("modelOrd");

			lb = Double.parseDouble(lbstr);
			rb = Double.parseDouble(rbstr);
			modOrd = Integer.parseInt(modOrdstr);

			String secBound[] = new String[3];

			for (Text val : values) {

				modelParser2(val.toString(), coef, range, secBound);

				step = ((range[1] - range[0]) / gridstep);
				st = range[0];
				ed = range[1];

				reskey = "";

				// //....put segment into the result table.......
				

				// .....put grided data points into the result table
				// if (step > 0) {
				for (double i = st; i <= ed; i++) {

					if (i >= lb && i <= rb) {

						put = new Put(Bytes.toBytes(Double.toString(i)));
						put.add(CF, VAL,
								Bytes.toBytes(Double.toString(coef[0])));

						context.write(null, put);
					}

					// res[num] = coef[0];
					// respoint = coef[0];

					// if()

					// put = new
					// Put(Bytes.toBytes(Integer.toString((int)i)));
					// put.add(Bytes.toBytes("attri"),
					// Bytes.toBytes("results"),
					// Bytes.toBytes(Double.toString(respoint)));
					//
					// context.write(null, put);
				}
				// }

			}

		}
	}

	public static class ReducerInterVal extends // for interval query
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

		public double cntnum = 0;
		public double[] coef = new double[10], range = new double[5];
		public double[] res = new double[100];
		public double respoint = 0.0;

		public double step = 0.0;
		public double st = 0.0, ed = 0.0;
		String reskey = new String();

		public double gridstep = 5;// 30.0;

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		Put put;

		
		
		
		byte[] CFA = "attri".getBytes();
		byte[] VL = "vl".getBytes();
		byte[] VR = "vr".getBytes();

		byte[] CFM = "model".getBytes();
		byte[] VAL = "val".getBytes();
		
		
		
		public void modelParser2(String modelstr, double coef[],
				double range[], String secRange[])// range[0]:
		// left time, right time, left value, right value
		{

			int len = modelstr.length(), st = 0, num = 0;
			
			for (int i = 0; i < len; ++i) {
				if (modelstr.charAt(i) == ';') {

					if (num < 2) {
						range[num++] = Double.parseDouble(modelstr.substring(
								st, i));
						st = i + 1;
					} else if (num >= 2) {
						secRange[num - 2] = modelstr.substring(st, i);
						num++;
						st = i + 1;
					
					} else if (num >= 4) {
						st = i + 1;
						break;
					}
				}
			}

			coef[0] = Double.parseDouble(modelstr.substring(st, len));

			return;
		}
		

//		public void modelParser2(String modelstr, double coef[], double range[])// range[0]:
//		// left
//		{
//
//			int len = modelstr.length(), st = 0, num = 0;
//			for (int i = 0; i < len; ++i) {
//				if (modelstr.charAt(i) == ',') {
//					range[num++] = Double
//							.parseDouble(modelstr.substring(st, i));
//					st = i + 1;
//					if (modelstr.charAt(i) == ',' && num >= 4) { // modification
//
//						// num++;
//						st = i + 1;
//						break;
//					}
//				}
//			}
//
//			coef[0] = Double.parseDouble(modelstr.substring(st, len));
//
//			return;
//		}

		// @Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			lbstr = conf.get("lb");
			rbstr = conf.get("rb");
			modOrdstr = conf.get("modelOrd");

			// lb=Double.valueOf(lbstr);
			lb = Double.parseDouble(lbstr);
			rb = Double.parseDouble(rbstr);
			modOrd = Integer.parseInt(modOrdstr);
			
			String secBound[] = new String[3];
			
			
			for (Text val : values) {

				modelParser2(val.toString(), coef, range, secBound);
				reskey = "";
				
				//........put segments into the result table....
				
				
				put = new Put(Bytes.toBytes(secBound[0] + "," + secBound[1]));

				put.add(CFA, VL, Bytes.toBytes(Double.toString(range[0])));

				put.add(CFA, VR, Bytes.toBytes(Double.toString(range[1])));

				put.add(CFM, VAL, Bytes.toBytes(Double.toString(coef[0])));

				context.write(null, put);
				
				
				//........put data points into the result table...
				
//				step = ((range[1] - range[0]) / gridstep);
//				st = range[0];
//				ed = range[1];
//				
//				// if (step > 0) {
//
//				if (coef[0] >= lb && coef[0] <= rb)
//
//					for (double i = st; i <= ed; i++) {
//
//						put = new Put(Bytes.toBytes(Double.toString(i)));
//						put.add(CF, VAL,
//								Bytes.toBytes(Double.toString(coef[0])));
//
//						context.write(null, put);
//
//						// res[num] = coef[0];
//						// respoint = coef[0];
//
//						// if()
//
//						// put = new
//						// Put(Bytes.toBytes(Integer.toString((int)i)));
//						// put.add(Bytes.toBytes("attri"),
//						// Bytes.toBytes("results"),
//						// Bytes.toBytes(Double.toString(respoint)));
//						//
//						// context.write(null, put);
//					}
				
				//................................................

			}

		}
	}

	public static class ReducerPointTemp extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

		public double cntnum = 0;
		public double[] coef = new double[10], range = new double[5];
		public double[] res = new double[100];
		public double respoint = 0.0;

		public double step = 0.0;
		public double st = 0.0, ed = 0.0;
		String reskey = new String();

		public double gridstep = 1;// 30.0;

		String valstr = new String();
		String modOrdstr = new String();
		double val = 0.0;
		int modOrd = 0;

		Put put;
		byte[] CF = "attri".getBytes();
		byte[] VAL = "val".getBytes();

		public void modelParser2(String modelstr, double coef[], double range[])// range[0]:
		// left
		{

			int len = modelstr.length(), st = 0, num = 0;
			for (int i = 0; i < len; ++i) {
				if (modelstr.charAt(i) == ',') {
					range[num++] = Double
							.parseDouble(modelstr.substring(st, i));
					st = i + 1;
					if (modelstr.charAt(i) == ',' && num >= 4) { // modification

						// num++;
						st = i + 1;
						break;
					}
				}
			}

			coef[0] = Double.parseDouble(modelstr.substring(st, len));

			return;
		}

		// @Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			valstr = conf.get("val");
			modOrdstr = conf.get("modelOrd");

			val = Double.parseDouble(valstr);
			modOrd = Integer.parseInt(modOrdstr);

			for (Text str : values) {

				modelParser2(str.toString(), coef, range);

				step = ((range[1] - range[0]) / gridstep);
				st = range[0];
				ed = range[1];
				int num = 0;

				reskey = "";

				if (val >= st && val <= ed) {

					put = new Put(Bytes.toBytes(Double.toString(val)));
					put.add(CF, VAL, Bytes.toBytes(Double.toString(coef[0])));

					context.write(null, put);
				}

				// res[num] = coef[0];
				// respoint = coef[0];

				// put = new
				// Put(Bytes.toBytes(Integer.toString((int)i)));
				// put.add(Bytes.toBytes("attri"),
				// Bytes.toBytes("results"),
				// Bytes.toBytes(Double.toString(respoint)));
				//
				// context.write(null, put);
				// }
				// }

			}

		}
	}

	public static class ReducerPointVal extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

		public double cntnum = 0;
		public double[] coef = new double[10], range = new double[5];
		public double[] res = new double[100];
		public double respoint = 0.0;

		public double step = 0.0;
		public double st = 0.0, ed = 0.0;
		String reskey = new String();

		public double gridstep = 1;// 30.0;

		String valstr = new String();
		String modOrdstr = new String();
		double val = 0.0;
		int modOrd = 0;

		Put put;
		byte[] CF = "attri".getBytes();
		byte[] VAL = "val".getBytes();

		public void modelParser2(String modelstr, double coef[], double range[])// range[0]:
		// left
		{

			int len = modelstr.length(), st = 0, num = 0;
			for (int i = 0; i < len; ++i) {
				if (modelstr.charAt(i) == ',') {
					range[num++] = Double
							.parseDouble(modelstr.substring(st, i));
					st = i + 1;
					if (modelstr.charAt(i) == ',' && num >= 4) { // modification

						// num++;
						st = i + 1;
						break;
					}
				}
			}

			coef[0] = Double.parseDouble(modelstr.substring(st, len));

			return;
		}

		// @Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			valstr = conf.get("val");
			modOrdstr = conf.get("modelOrd");

			val = Double.parseDouble(valstr);
			modOrd = Integer.parseInt(modOrdstr);

			for (Text str : values) {

				modelParser2(str.toString(), coef, range);

				step = ((range[1] - range[0]) / gridstep);
				st = range[0];
				ed = range[1];
				int num = 0;

				reskey = "";
				// if (step > 0) {

				if (Math.abs(coef[0] - val) < 1e-1) {
					for (double i = st; i <= ed; i += step) {

						// if (i >= lb && i <= rb) {

						put = new Put(Bytes.toBytes(Double.toString(i)));
						put.add(CF, VAL,
								Bytes.toBytes(Double.toString(coef[0])));

						context.write(null, put);
						// }

						// res[num] = coef[0];
						// respoint = coef[0];

						// put = new
						// Put(Bytes.toBytes(Integer.toString((int)i)));
						// put.add(Bytes.toBytes("attri"),
						// Bytes.toBytes("results"),
						// Bytes.toBytes(Double.toString(respoint)));
						//
						// context.write(null, put);
					}
				}
				// }

			}

		}
	}

	// public double CostjobIndiviRegIntervalEvalQuery(double lb, double rb,
	// String idxtabname, String strow, String edrow, String col[],
	// double runpara[]) throws Exception {
	//
	// long st = 0, ed = 0;
	//
	// Configuration conf = HBaseConfiguration.create();
	//
	// conf.set("lb", Double.toString(lb));
	// conf.set("rb", Double.toString(rb));
	// conf.set("modelOrd", Integer.toString(modorder));
	//
	// Job job = new Job(conf, "job");
	// job.setJarByClass(kviMRproCol.class);
	// job.setNumReduceTasks(nored);
	//
	// Scan scan = new Scan();
	// scan.setCaching(scanCache);
	// scan.setCacheBlocks(false);
	//
	// scan.setStartRow(Bytes.toBytes(strow));
	// scan.setStopRow(Bytes.toBytes(edrow));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
	// scan.addFamily(Bytes.toBytes("model"));
	//
	// job.setInputFormatClass(RegIndiviTableInputFormat.class);
	//
	// RegIndiviTableInputFormat cosCal = new RegIndiviTableInputFormat();
	//
	// if (col[0] == "st") {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperTempInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegIndiviTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	//
	// } else {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperValInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegIndiviTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	// }
	//
	// return cosCal.calSplits();
	//
	// // boolean done = job.waitForCompletion(true);
	// // if (!done) {
	// // throw new IOException("error with job!");
	// // }
	// //
	// // runpara[0] = job
	// // .getCounters()
	// // .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// // "MAP_OUTPUT_RECORDS").getValue();
	// // runpara[1] = job
	// // .getCounters()
	// // .findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
	// // "TOTAL_LAUNCHED_MAPS").getValue();
	// //
	// // return job
	// // .getCounters()
	// // .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// // "MAP_INPUT_RECORDS").getValue();
	//
	// }

	public double jobPointEvalQuery(double val, String idxtabname,
			String strow, String edrow, String col[], double runpara[])
			throws Exception {

		long st = 0, ed = 0;

		Configuration conf = HBaseConfiguration.create();

		conf.set("val", Double.toString(val));
		conf.set("modelOrd", Integer.toString(modorder));

		// conf.set("reducernum", Double.toString(rednum));

		Job job = new Job(conf, "job");
		job.setJarByClass(kviMRproCol.class);// modification
		// job.getConfiguration().setInt("mapred.map.tasks", 2000);
		// job.setNumReduceTasks(nored);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);

		// .....test.............//
		// System.out.printf("%s    %s\n",col[0],col[1]);

		// ......................//

		scan.setStartRow(Bytes.toBytes(strow));
		scan.setStopRow(Bytes.toBytes(edrow));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
		scan.addFamily(Bytes.toBytes("model"));

		if (col[0] == "st") {

			job.setNumReduceTasks(1);
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperTempPoint.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "-qryRes",
					ReducerPointTemp.class, job);

		} else {

			job.setNumReduceTasks(1);
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperValPoint.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "-qryRes",
					ReducerPointVal.class, job);
		}

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

		runpara[0] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getValue();
		runpara[1] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
						"TOTAL_LAUNCHED_MAPS").getValue();

		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();

		// TOTAL_LAUNCHED_MAPS

	}

	// public double jobIntervalEvalQuery(double lb, double rb, String
	// idxtabname,
	// String strow, String edrow, String col[], double runpara[])
	// throws Exception {
	//
	// long st = 0, ed = 0;
	//
	// Configuration conf = HBaseConfiguration.create();
	//
	// conf.set("lb", Double.toString(lb));
	// conf.set("rb", Double.toString(rb));
	// conf.set("modelOrd", Integer.toString(modorder));
	//
	// Job job = new Job(conf, "job");
	// job.setJarByClass(kviMRproCol.class);// modification
	// // job.getConfiguration().setInt("mapred.map.tasks", 2000);
	// job.setNumReduceTasks(nored);
	//
	// Scan scan = new Scan();
	// scan.setCaching(scanCache);
	// scan.setCacheBlocks(false);
	//
	// scan.setStartRow(Bytes.toBytes(strow));
	// scan.setStopRow(Bytes.toBytes(edrow));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
	// scan.addFamily(Bytes.toBytes("model"));
	//
	// job.setInputFormatClass(RegTableInputFormat.class);
	//
	// if (col[0] == "st") {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperTempInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	//
	// } else {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperValInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	// }
	//
	// boolean done = job.waitForCompletion(true);
	// if (!done) {
	// throw new IOException("error with job!");
	// }
	//
	// runpara[0] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_OUTPUT_RECORDS").getValue();
	// runpara[1] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
	// "TOTAL_LAUNCHED_MAPS").getValue();
	//
	// return job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_INPUT_RECORDS").getValue();
	//
	// // TOTAL_LAUNCHED_MAPS
	//
	// }

	// public double jobScanRegIntervalEvalQuery(double lb, double rb,
	// String idxtabname, String strow, String edrow, String col[],
	// double runpara[]) throws Exception {
	//
	// long st = 0, ed = 0;
	//
	// Configuration conf = HBaseConfiguration.create();
	//
	// conf.set("lb", Double.toString(lb));
	// conf.set("rb", Double.toString(rb));
	// conf.set("modelOrd", Integer.toString(modorder));
	//
	// Job job = new Job(conf, "job");
	// job.setJarByClass(kviMRproCol.class);
	// job.setNumReduceTasks(nored);
	//
	// Scan scan = new Scan();
	// scan.setCaching(scanCache);
	// scan.setCacheBlocks(false);
	//
	// scan.setStartRow(Bytes.toBytes(strow));
	// scan.setStopRow(Bytes.toBytes(edrow));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
	// scan.addFamily(Bytes.toBytes("model"));
	//
	// job.setInputFormatClass(RegScanTableInputFormat.class);
	//
	// if (col[0] == "st") {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperTempInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegScanTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	//
	// } else {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperValInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegScanTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	// }
	//
	// boolean done = job.waitForCompletion(true);
	// if (!done) {
	// throw new IOException("error with job!");
	// }
	//
	// runpara[0] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_OUTPUT_RECORDS").getValue();
	// runpara[1] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
	// "TOTAL_LAUNCHED_MAPS").getValue();
	//
	// return job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_INPUT_RECORDS").getValue();
	//
	// }

	public double jobIndiviRegIntervalEvalQuery(double lb, double rb,
			String idxtabname, String strow, String edrow, String col[],
			double runpara[]) throws Exception {

		long st = 0, ed = 0;

		Configuration conf = HBaseConfiguration.create();

		conf.set("lb", Double.toString(lb));
		conf.set("rb", Double.toString(rb));
		conf.set("modelOrd", Integer.toString(modorder));

		Job job = new Job(conf, "job");
		job.setJarByClass(kviMRproCol.class);
		job.setNumReduceTasks(nored);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);

		scan.setStartRow(Bytes.toBytes(strow));
		scan.setStopRow(Bytes.toBytes(edrow));
		// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
		// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));

		scan.addFamily(Bytes.toBytes("attri"));
		scan.addFamily(Bytes.toBytes("model"));

		job.setInputFormatClass(RegIndiviTableInputFormat.class);

		if (col[0] == "st") {
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperTempInterval.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegIndiviTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "-qryRes",
					ReducerInterTemp.class, job);

		} else {
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperValInterval.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegIndiviTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "-qryRes",
					ReducerInterVal.class, job);
		}

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

		runpara[0] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getValue();
		runpara[1] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
						"TOTAL_LAUNCHED_MAPS").getValue();

		runpara[2] = RegIndiviTableInputFormat.staTsplits;

		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();

	}

	// public double jobAdaRegIntervalEvalQuery(double lb, double rb,
	// String idxtabname, String strow, String edrow, String col[],
	// double runpara[]) throws Exception {
	//
	// long st = 0, ed = 0;
	//
	// Configuration conf = HBaseConfiguration.create();
	//
	// conf.set("lb", Double.toString(lb));
	// conf.set("rb", Double.toString(rb));
	// conf.set("modelOrd", Integer.toString(modorder));
	//
	// Job job = new Job(conf, "job");
	// job.setJarByClass(kviMRproCol.class);
	// job.setNumReduceTasks(nored);
	//
	// Scan scan = new Scan();
	// scan.setCaching(scanCache);
	// scan.setCacheBlocks(false);
	//
	// scan.setStartRow(Bytes.toBytes(strow));
	// scan.setStopRow(Bytes.toBytes(edrow));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
	// scan.addFamily(Bytes.toBytes("model"));
	//
	// job.setInputFormatClass(RegIndiviTableInputFormat.class);
	//
	// if (col[0] == "st") {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperTempInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegIndiviTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	//
	// } else {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperValInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegIndiviTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	// }
	//
	// boolean done = job.waitForCompletion(true);
	// if (!done) {
	// throw new IOException("error with job!");
	// }
	//
	// runpara[0] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_OUTPUT_RECORDS").getValue();
	// runpara[1] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
	// "TOTAL_LAUNCHED_MAPS").getValue();
	//
	// return job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_INPUT_RECORDS").getValue();
	//
	// }

	// public double jobAdaBaseRegIntervalEvalQuery(double lb, double rb,
	// String idxtabname, String strow, String edrow, String col[],
	// double runpara[]) throws Exception {
	//
	// long st = 0, ed = 0;
	//
	// Configuration conf = HBaseConfiguration.create();
	//
	// conf.set("lb", Double.toString(lb));
	// conf.set("rb", Double.toString(rb));
	// conf.set("modelOrd", Integer.toString(modorder));
	//
	// Job job = new Job(conf, "job");
	// job.setJarByClass(kviMRproCol.class);
	// job.setNumReduceTasks(nored);
	//
	// Scan scan = new Scan();
	// scan.setCaching(scanCache);
	// scan.setCacheBlocks(false);
	//
	// scan.setStartRow(Bytes.toBytes(strow));
	// scan.setStopRow(Bytes.toBytes(edrow));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
	// scan.addFamily(Bytes.toBytes("model"));
	//
	// job.setInputFormatClass(RegIndiviTableInputFormat.class);
	//
	// if (col[0] == "st") {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperTempInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegAdaBaseTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	//
	// } else {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperValInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegAdaBaseTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	// }
	//
	// boolean done = job.waitForCompletion(true);
	// if (!done) {
	// throw new IOException("error with job!");
	// }
	//
	// runpara[0] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_OUTPUT_RECORDS").getValue();
	// runpara[1] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
	// "TOTAL_LAUNCHED_MAPS").getValue();
	//
	// return job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_INPUT_RECORDS").getValue();
	//
	// }

	// public double jobMergeReg_test(double lb, double rb, String idxtabname,
	// String strow, String edrow, String col[], double runpara[])
	// throws Exception {
	//
	// long st = 0, ed = 0;
	//
	// Configuration conf = HBaseConfiguration.create();
	//
	// conf.set("lb", Double.toString(lb));
	// conf.set("rb", Double.toString(rb));
	// conf.set("modelOrd", Integer.toString(modorder));
	//
	// Job job = new Job(conf, "job");
	// job.setJarByClass(kviMRproCol.class);
	// job.setNumReduceTasks(nored);
	//
	// Scan scan = new Scan();
	// scan.setCaching(scanCache);
	// scan.setCacheBlocks(false);
	//
	// // scan.setStartRow(Bytes.toBytes(strow));
	// // scan.setStopRow(Bytes.toBytes(edrow));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
	// scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
	// scan.addFamily(Bytes.toBytes("model"));
	//
	// job.setInputFormatClass(RegIndiviTableInputFormat.class);
	//
	// if (col[0] == "st") {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperTempInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegMergeTableInputFormat_test.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	//
	// } else {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperValInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegMergeTableInputFormat_test.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	// }
	//
	// boolean done = job.waitForCompletion(true);
	// if (!done) {
	// throw new IOException("error with job!");
	// }
	//
	// runpara[0] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_OUTPUT_RECORDS").getValue();
	// runpara[1] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
	// "TOTAL_LAUNCHED_MAPS").getValue();
	//
	// return job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_INPUT_RECORDS").getValue();
	//
	// }

	// public double jobComposIntervalEvalQuery(double tlb, double trb,
	// double vlb, double vrb, String idxtabname, String strow,
	// String edrow, String col[], double runpara[]) throws Exception {
	//
	// long st = 0, ed = 0;
	//
	// Configuration conf = HBaseConfiguration.create();
	//
	// conf.set("tlb", Double.toString(tlb));
	// conf.set("trb", Double.toString(trb));
	// conf.set("vlb", Double.toString(vlb));
	// conf.set("vrb", Double.toString(vrb));
	//
	// conf.set("modelOrd", Integer.toString(modorder));
	//
	// Job job = new Job(conf, "job");
	// job.setJarByClass(kviMRproCol.class);
	// job.setNumReduceTasks(nored);
	//
	// Scan scan = new Scan();
	// scan.setCaching(scanCache);
	// scan.setCacheBlocks(false);
	//
	// scan.setStartRow(Bytes.toBytes(strow));
	// scan.setStopRow(Bytes.toBytes(edrow));
	// // scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
	// // scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
	// scan.addFamily(Bytes.toBytes("attri"));
	// scan.addFamily(Bytes.toBytes("model"));
	//
	// job.setInputFormatClass(RegIndiviTableInputFormat.class);
	//
	// if (col[0] == "st") {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperComposInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegIndiviTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	//
	// } else {
	// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
	// MapperComposInterval.class, ImmutableBytesWritable.class,
	// Text.class, job, true, RegIndiviTableInputFormat.class);
	//
	// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
	// ReducerEval.class, job);
	// }
	//
	// boolean done = job.waitForCompletion(true);
	// if (!done) {
	// throw new IOException("error with job!");
	// }
	//
	// runpara[0] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_OUTPUT_RECORDS").getValue();
	// runpara[1] = job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
	// "TOTAL_LAUNCHED_MAPS").getValue();
	//
	// return job
	// .getCounters()
	// .findCounter("org.apache.hadoop.mapred.Task$Counter",
	// "MAP_INPUT_RECORDS").getValue();
	//
	// }

}
