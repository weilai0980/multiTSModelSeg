//package basetool;
//
//
//
//
//public class kviManEnhan {
//
//}
package basetool;

import java.io.*;
import java.util.*;

import basetool.kviMRenhance.RegAdaBaseTableInputFormat;
import basetool.kviMRenhance.RegAdaTableInputFormat;
import basetool.kviMRenhance.RegIndiviTableInputFormat;
import basetool.kviMRenhance.RegScanTableInputFormat;
import basetool.kviMRenhance.RegTableInputFormat;

public class kviManEnhan {

	public kviIndexCol kviCol;
	public kviMRproCol kviqueryCol;

	public kviMRenhance kvimrpro;

	public String idxtabname;
	public scanPro tabscan;
	public long tduration;
	public double numtup;
	public String tupRangeL, tupRangeR;
	public long inmem, randacc, mracc;
	public long schlevel;
	public double parall;
	public double mapout, mapcnt;
	public int mapsplits;
	public double qrycost;
	public String strk, edrk;

	public nodeDistri nodeDist;

	public int mapNumCons = 96;

	public int grelregnum = 0;
	public long[] grelReg = new long[100];
	public double[][] gninReg = new double[100][20];
	public int[] gninRegCnt = new int[100];

	// relregnum, releReg, gnodeinReg,
	// nodeinRegCnt);

	public kviManEnhan(String tabname,int sign, int isIni)
			throws IOException {// 0: temp 1:val
		// kvi = new kviIndex(tabname);
		kviCol = new kviIndexCol(tabname);

//		if (startIdx == true) {

			kviCol.iniIdx(isIni);
//		} else {
//
//			kviCol.iniQueryRes();
//		}

		kviqueryCol = new kviMRproCol(0, tabname, sign);
		kvimrpro = new kviMRenhance(0, tabname, sign);

		idxtabname = tabname;
		tabscan = new scanPro(0);
		tduration = 0;
		schlevel = 0;

		numtup = 0.0;
		tupRangeL = tupRangeR = "";
		inmem = randacc = mracc = 0;

		mapout = 0.0;
		mapcnt = 0.0;
		mapsplits=0;
		qrycost=0.0;

		parall = 3.0;

		strk = new String();
		edrk = new String();

//		nodeDist 
//		= new nodeDistri(tabname + "Dis-guo");

		mapNumCons = 96;
		

	}

	// public boolean kvi_insert(double l, double r, double modinfo[], int
	// order,
	// double assocl, double assocr, double timecons[]) {
	//
	// double[] timecnt = new double[4];
	// if (kvi.insert(l, r, modinfo, order, assocl, assocr, timecnt) == true) {
	//
	// timecons[0] = timecnt[0];
	// timecons[1] = timecnt[1];
	//
	// return true;
	// } else {
	// return false;
	// }
	// }
	public boolean kvi_insert(double l, double r, double modinfo[], int order,
			double assocl, double assocr, double timecons[], String qual[],
			String assoQual[]) {

		double[] timecnt = new double[4];
		if (kviCol.insert(l, r, modinfo, order, assocl, assocr, timecnt, qual,
				assoQual) == true) {

			timecons[0] = timecnt[0];
			timecons[1] = timecnt[1];

			return true;
		} else {
			return false;
		}
	}

	public boolean kvi_pointSearch(double val, String qual[], int qrytype) {// 0:
																			// temp
																			// 1:
																			// value
		String[][] indi = new String[100][2];
		long stcnt = 0, edcnt = 0;

		stcnt = System.nanoTime();
		int resnum = kviCol.pointSearch_res(indi, val);

		// ..........modification..........................//

		numtup = tabscan.pointSearch_scan(idxtabname, indi, val, resnum, qual,
				qrytype);

		edcnt = System.nanoTime();

		// ..........metric.......................//
		tduration = (long) ((edcnt - stcnt) / parall);

		return true;
	}

	public boolean kvi_pointSearch_mr(double val, String qual[], int qrytype)
			throws Exception {// 0:
		// temp
		// 1:
		// value
		String[] bdrow = new String[5];
		double[] runpara = new double[10];
		long stcnt = 0, edcnt = 0;

		String[] tcol = { "st", "ed" }, vcol = { "vl", "vr" };

		stcnt = System.nanoTime();

		int resnum = kviCol.pointSearch_resMr(bdrow, val, qrytype);

		// ......test................//
		// System.out.printf("%s   %s\n", bdrow[0],bdrow[1]);

		if (qrytype == 0) {
			kviqueryCol.jobPointEvalQuery(val, idxtabname, bdrow[0], bdrow[1],
					tcol, runpara);
		} else {
			kviqueryCol.jobPointEvalQuery(val, idxtabname, bdrow[0], bdrow[1],
					vcol, runpara);
		}
		edcnt = System.nanoTime();

		// ..........metric.......................//
		tduration = (long) ((edcnt - stcnt));

		return true;
	}

	public double kvi_timeCnt() {
		return tduration / 1000000000.0;
	}

	public void kvi_metricOutput(double stat[], String rowrange[]) {
		stat[0] = tduration / 1000000000.0;
		stat[1] = inmem / 1000000000.0;
		stat[2] = randacc / 1000000000.0;
		stat[3] = mracc / 1000000000.0;
		stat[4] = numtup;
		stat[5] = schlevel;

		stat[6] = mapout;
		stat[7] = mapcnt;
		stat[8] = qrycost;
		
//		stat[9]= qrycost;
		

		rowrange[0] = tupRangeL;
		rowrange[1] = tupRangeR;
		return;
	}

	public void kvi_metricOutput(double stat[]) {
		stat[0] = tduration / 1000000000.0;
		stat[1] = numtup;
		return;
	}

	// public boolean kvi_rangeSearch(double l, double r) {
	//
	// String[][] indi = new String[100][2];
	// String[][] parallel = new String[100][2];
	// long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
	//
	//
	// // ..test..//
	// double tmp = 0.0;
	//
	// stcnt = System.nanoTime();
	//
	// int resnum = kvi.intervalSearch_res(indi, parallel, l, r);
	//
	// inmemCnt = System.nanoTime();
	//
	// try {
	// kviMRpro.numMapIn = 0;
	// tmp = kviquery.jobIntervalEvalQuery(l, r, idxtabname,
	// parallel[0][0], parallel[1][1]);
	//
	// // tmp = kviqueryCol.jobIntervalEvalQuery(l, r, idxtabname,
	// // parallel[0][0], parallel[1][1]);
	//
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// midcnt = System.nanoTime();
	//
	// numtup = tabscan.rangeSearch_scan(idxtabname, indi, l, r, resnum) + tmp;
	//
	// edcnt = System.nanoTime();
	//
	// tupRangeL = parallel[0][0];
	// tupRangeR = parallel[1][1];
	//
	// // ......metric and running time record............//
	// tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
	// inmem = inmemCnt - stcnt;
	// mracc = midcnt - inmemCnt;
	// randacc = edcnt - midcnt;
	// schlevel = resnum;
	//
	// return true;
	// }
//	public boolean kvi_rangeEnhan(double l, double r, String qual[], int sign)// 0:
//			// temp,
//			// 1:value
//			throws Exception {
//
//		String[][] indi = new String[100][2];
//		String[][] parallel = new String[100][2];
//		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
//		double[] runpara = { 0.0, 0.0 };
//		double tmp = 0.0;
//		int resnum = 0;
//
//		long[] releReg = new long[200];
//
//		stcnt = System.nanoTime();
//		int relregnum = kviCol.relRegIntervalSearch(l, r, releReg);
//		// int resnum = intervalSearch_res(indi, parallel, l, r);
//
//		// ...........test..........................//
//
//		// System.out.printf("relevant regions:");
//		// for(int i=0;i<relregnum;++i)
//		// {
//		// System.out.printf("%d, ",releReg[i]);
//		// }
//		// System.out.printf("\n");
//
//		System.out.printf("number of regions: %d \n", relregnum);
//
//		// .........................................//
//
//		// transform releReg to continuous region ID
//		kvimrpro.getRelReg(releReg, relregnum);
//		kviMRproCol.numMapIn = 0;
//
//		inmemCnt = System.nanoTime();
//
//		RegTableInputFormat.getRegion(relregnum, releReg);
//
//		tmp = kvimrpro.jobIntervalEvalQuery(l, r, idxtabname,
//				kviCol.regvalKeyCon((long) l) + ",0",
//				kviCol.regvalKeyCon((long) r + 1) + ",0", qual, runpara);
//
//		midcnt = System.nanoTime();
//
//		// modification
//		numtup = tmp;
//
//		edcnt = System.nanoTime();
//
//		// ......metric record............//
//		tupRangeL = parallel[0][0];
//		tupRangeR = parallel[1][1];
//
//		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
//		inmem = inmemCnt - stcnt;
//		mracc = midcnt - inmemCnt;
//		randacc = edcnt - midcnt;
//		schlevel = resnum;
//
//		mapout = runpara[0];
//		mapcnt = runpara[1];
//
//		return true;
//	}

//	public boolean kvi_rangeScanPerReg(double l, double r, String qual[],
//			int sign)// 0:
//			// temp,
//			// 1:value
//			throws Exception {
//
//		String[][] indi = new String[100][2];
//		String[][] parallel = new String[100][2];
//		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
//		double[] runpara = { 0.0, 0.0 };
//		double tmp = 0.0;
//		int resnum = 0;
//
//		long[] releReg = new long[200];
//		String[][] scanReg = new String[200][3];
//
//		stcnt = System.nanoTime();
//		int relregnum = kviCol.relScanRegIntervalSearch(l, r, releReg, scanReg);
//
//		System.out.printf("number of regions: %d \n", relregnum);
//
//		// .........................................//
//
//		kvimrpro.getRelReg(releReg, relregnum);
//		kviMRproCol.numMapIn = 0;
//
//		inmemCnt = System.nanoTime();
//
//		RegScanTableInputFormat.getRegion(relregnum, releReg, scanReg);
//
//		tmp = kvimrpro.jobScanRegIntervalEvalQuery(l, r, idxtabname,
//				kviCol.regvalKeyCon((long) l) + ",0",
//				kviCol.regvalKeyCon((long) r + 1) + ",0", qual, runpara);
//
//		midcnt = System.nanoTime();
//
//		edcnt = System.nanoTime();
//
//		// ......metric record............//
//		tupRangeL = parallel[0][0];
//		tupRangeR = parallel[1][1];
//
//		numtup = tmp;
//		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
//		inmem = inmemCnt - stcnt;
//		mracc = midcnt - inmemCnt;
//		randacc = edcnt - midcnt;
//		schlevel = resnum;
//
//		mapout = runpara[0];
//		mapcnt = runpara[1];
//
//		return true;
//	}

//	public int kvi_CostRangeIndiviPerReg(double l, double r, String qual[],
//			int sign)// 0:
//			// temp,
//			// 1:value
//			throws Exception {
//
//		// String[][] indi = new String[100][2];
//		// String[][] parallel = new String[100][2];
//		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
//		double[] runpara = { 0.0, 0.0 };
//		double tmp = 0.0;
//		int resnum = 0;
//
//		long[] releReg = new long[200];
//		// String[][] scanReg = new String[200][3];
//
//		double[][] nodeinReg = new double[200][100];
//		int[] nodeinRegCnt = new int[200];
//
//		double[] extInteNode = new double[100];
//		stcnt = System.nanoTime();
//		int relregnum = kviCol.relIndiviRegIntervalSearch(l, r, releReg,
//				nodeinReg, nodeinRegCnt, extInteNode);
//
//		System.out.printf("number of regions: %d \n", relregnum);
//
//	
//		//......................test......................//
//		int tsplit=0;
//		
//		for(int i=0;i<relregnum;++i)
//		{
//			System.out.printf("split in each region: %d\n", nodeinRegCnt[i]);
//			tsplit+=nodeinRegCnt[i];
//		}
//		
//		// .........................................//
//		
//		return 0;
//		
//		
////		inmemCnt = System.nanoTime();
////
////		RegIndiviTableInputFormat.getRegion(relregnum, releReg, nodeinReg,
////				nodeinRegCnt);
////
//////		RegIndiviTableInputFormat costEst= new RegIndiviTableInputFormat();
////		return (int)kvimrpro.CostjobIndiviRegIntervalEvalQuery(l, r, idxtabname,
////				kviCol.regvalKeyCon((long) l) + ",0",
////				kviCol.regvalKeyCon((long) r + 1) + ",0", qual, runpara);
//		
////		tmp = kvimrpro.jobIndiviRegIntervalEvalQuery(l, r, idxtabname,
////				kviCol.regvalKeyCon((long) l) + ",0",
////				kviCol.regvalKeyCon((long) r + 1) + ",0", qual, runpara);
////
////		midcnt = System.nanoTime();
////
////		edcnt = System.nanoTime();
////
////		// ......metric record............//
////		// tupRangeL = parallel[0][0];
////		// tupRangeR = parallel[1][1];
////
////		numtup = tmp;
////		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
////		inmem = inmemCnt - stcnt;
////		mracc = midcnt - inmemCnt;
////		randacc = edcnt - midcnt;
////		schlevel = resnum;
////
////		mapout = runpara[0];
////		mapcnt = runpara[1];
//
////		return tsplit;
//	}

	public boolean kvi_rangeIndiviPerReg(double l, double r, String qual[],
			int sign)// 0:
			// temp,
			// 1:value
			throws Exception {

		// String[][] indi = new String[100][2];
		// String[][] parallel = new String[100][2];
		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
		double[] runpara = { 0.0, 0.0,0.0,0.0 };
		double tmp = 0.0;
		int resnum = 0;

		long[] releReg = new long[200];
		// String[][] scanReg = new String[200][3];

		double[][] nodeinReg = new double[200][100];
		int[] nodeinRegCnt = new int[200];

		double[] extInteNode = new double[100];
		stcnt = System.nanoTime();
		int relregnum = kviCol.relIndiviRegIntervalSearch(l, r, releReg,
				nodeinReg, nodeinRegCnt, extInteNode);

		System.out.printf("number of regions: %d \n", relregnum);

		// ......................test......................//
		int tsplit = 0;

		for (int i = 0; i < relregnum; ++i) {
			System.out.printf("split in each region: %d\n", nodeinRegCnt[i]);
			tsplit += nodeinRegCnt[i];
		}

		// .........................................//

		inmemCnt = System.nanoTime();

		RegIndiviTableInputFormat.getRegion(relregnum, releReg, nodeinReg,
				nodeinRegCnt);

		tmp = kvimrpro.jobIndiviRegIntervalEvalQuery(l, r, idxtabname,
				kviCol.regvalKeyCon((long) l) + ",0",
				kviCol.regvalKeyCon((long) r + 1) + ",0", qual, runpara);
		
		
		//........cost calculation.........................................//
		
		int tmpcnt=RegIndiviTableInputFormat.staRanRegCnt;
		long tmpreg=0;
		int j=0;
		for(int i=0;i<tmpcnt;++i)
		{
			tmpreg=RegIndiviTableInputFormat.staRanReg[i];
			
			for( j=0;j<relregnum;++j)
			{
				if(releReg[j]==tmpreg)
				{
					break;
				}
			}
			
			if(j!=relregnum)
			{
				nodeinRegCnt[j]++;
			}
			else
			{
					
			}
			
		}
	  
	    double wave=runpara[2]/96.0;
	    int intw= (int)wave+1;
	    
	    double a=0.7, b=0.3;//b=0.5;
	    double cost=a*wave,dtcost=0.0, tmpdt=0.0;
		for(int i=0;i<relregnum;++i)
		{
			
			tmpdt=intw-nodeinRegCnt[i];
			if(tmpdt>=0)
			{}
			else
			{
				dtcost+=(-tmpdt);
			}
		    
		}
	    cost+=(b*dtcost);
		
		//..................................................................//
		
		

		midcnt = System.nanoTime();

		edcnt = System.nanoTime();

		// ......metric record............//
		// tupRangeL = parallel[0][0];
		// tupRangeR = parallel[1][1];

		numtup = tmp;
		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
		inmem = inmemCnt - stcnt;
		mracc = midcnt - inmemCnt;
		randacc = edcnt - midcnt;
		schlevel = resnum;

		mapout = runpara[0];
		mapcnt = runpara[1];
		mapsplits=(int)runpara[2];
		
		qrycost=cost;
		

		return true;
	}

	public boolean kvi_rangeMergeReg_test(double l, double r, String qual[],
			int sign)// 0:
			// temp,
			// 1:value
			throws Exception {

		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
		double[] runpara = { 0.0, 0.0 };
		double tmp = 0.0;
		int resnum = 0;

		// .......region ID test..........................//

		String tmprow = kviCol.regvalKeyCon((long) (1000000000)) + "," + "0";

		kviCol.idxop.iniTabOperation("tempIdxmodCol");

		long tmpregid = kviCol.idxop.regionStat("tempIdxmodCol", tmprow);

		System.out.printf("no existend region ID check: %d\n", tmpregid);

		// ...............................................//

		stcnt = System.nanoTime();

		// .........................................//

		inmemCnt = System.nanoTime();

		// tmp = kvimrpro.jobMergeReg_test(l, r, idxtabname,
		// kviCol.regvalKeyCon((long) l) + ",0",
		// kviCol.regvalKeyCon((long) r + 1) + ",0", qual, runpara);

		midcnt = System.nanoTime();

		edcnt = System.nanoTime();

		// ......metric record............//
		// tupRangeL = parallel[0][0];
		// tupRangeR = parallel[1][1];

		numtup = tmp;
		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
		inmem = inmemCnt - stcnt;
		mracc = midcnt - inmemCnt;
		randacc = edcnt - midcnt;
		schlevel = resnum;

		mapout = runpara[0];
		mapcnt = runpara[1];

		return true;
	}

	public double kvi_rangeAdaRedunCal(double lower, double upper,
			int speciIdx, double lb, double rb) {

		double res = 0.0;
		if (lower > upper) {
			res = lower;
			lower = upper;
			upper = res;
		}

		if (lower == speciIdx) {
			return nodeDist.bstRedunInterCap(rb, upper);
		} else {
			return nodeDist.bstRedunInterCap(lower, upper);
		}
	}

	public int kvi_rangeAdaOptPre(int num, double extpath[], double cpath[],
			double lb, double rb) {
		int cnt = 0, flag = 0, specIdx = 0;
		for (int i = 0; i < num; ++i) {
			if (flag == 1) {
				cpath[cnt++] = extpath[i];
			} else {
				if (extpath[i] < lb) {
					cpath[cnt++] = extpath[i];

				} else {
					cpath[cnt] = lb;
					specIdx = cnt;
					cnt++;
					cpath[cnt++] = extpath[i];
					flag = 1;
				}
			}
		}
		return specIdx;
	}

	public void kvi_rangeAdaOpt(int extPathcnt, double extPath[],
			double mergeNode[][], int mapCons, int specPos, double lb, double rb) {
		double[] exist = new double[100];
		double[] intCost = new double[100];

		int existcnt = 0, tmp = 0;
		for (int i = 0; i < extPathcnt; ++i) {
			if ((tmp = nodeDist.bstNodeSch(extPath[i], 0)) != -1) {
				exist[existcnt] = tmp;
				existcnt++;
			}
		}
		for (int i = 1; i < existcnt; ++i) {
			intCost[i] = nodeDist.bstRedunInterCap(exist[i - 1], exist[i]);
		}

		// ............dp optimization................//

		double[][] f = new double[100][3];
		double[][] fSig = new double[100][100];
		int cur = 0;

		// ....dp initialization.........//

		for (int i = 0; i < 100; ++i) {
			for (int j = 0; j < 3; ++j)
				f[i][j] = 0.0;
		}
		for (int i = 0; i < 100; ++i) {
			for (int j = 0; j < 100; ++j)
				fSig[i][j] = 0.0;
		}

		for (int i = 1; i <= existcnt; ++i) {
			f[i][1 - cur] = f[i - 1][1 - cur] + intCost[i - 1];
			fSig[i][1] = i;
		}
		f[0][cur] = 0.0;
		f[0][1 - cur] = 0.0;
		f[1][cur] = 0.0;
		f[1][1 - cur] = 0.0;

		fSig[0][1] = 0.0;
		fSig[0][2] = 0.0;
		fSig[1][1] = 0.0;
		fSig[1][2] = 0.0;

		double tmpsig = 0.0;
		for (int i = 2; i <= mapCons; ++i) {
			for (int j = 2; j <= existcnt; ++j) {

				if (f[j - 1][cur] > f[j - 2][1 - cur]) {
					f[j][cur] = f[j - 2][1 - cur];
					tmpsig = j - 2;
				} else {
					f[j][cur] = f[j - 1][cur];
					// f[j][cur] += kvi_rangeAdaRedunCal(exist[j - 1 - 1],
					// exist[j - 1]);
					tmpsig = fSig[j - 1][i];
				}

				f[j][cur] += kvi_rangeAdaRedunCal(exist[j - 1 - 1],
						exist[j - 1], specPos, lb, rb);
				//
				//
				// (double lower, double upper,
				// int speciIdx, double lb, double rb)

				fSig[j][i] = tmpsig;

				if (f[j][cur] > f[j - 1][1 - cur]) {
					f[j][cur] = f[j - 1][1 - cur];
					fSig[j][i] = j - 1;
				}
			}
			cur = 1 - cur;
		}
		// ............recover node merging.............//

		int var = 0;
		int varn = extPathcnt;
		for (int i = mapCons; i > 0; --i) {
			var = (int) fSig[varn][i] - 1;

			mergeNode[i][0] = exist[var];
			mergeNode[i][1] = exist[varn - 1];

			varn = var;

		}

		return;
	}

//	public boolean kvi_rangeAdaptiveReg(double l, double r, String qual[],
//			int sign)// 0:
//			// temp,
//			// 1:value
//			throws Exception {
//
//		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
//		double[] runpara = { 0.0, 0.0 };
//		double tmp = 0.0;
//		int resnum = 0;
//
//		long[] releReg = new long[200];
//		double[] extInteNode = new double[100];
//		double[] nodepath = new double[100];
//
//		double[][] nodeinReg = new double[200][100];
//		int[] nodeinRegCnt = new int[200];
//
//		stcnt = System.nanoTime();
//		int relregnum = kviCol.relIndiviRegIntervalSearch(l, r, releReg,
//				nodeinReg, nodeinRegCnt, extInteNode);
//
//		inmemCnt = System.nanoTime();
//
//		System.out.printf("number of regions: %d \n", relregnum);
//
//		// ..............cost adaptive component..................//
//
//		// RegIndiviTableInputFormat.getRegion(relregnum, releReg, nodeinReg,
//		// nodeinRegCnt);
//		//
//		// RegIndiviTableInputFormat tmpInput = new RegIndiviTableInputFormat();
//		// int splitcnt = tmpInput.splitsCal();
//
//		midcnt = System.nanoTime();
//
//		// if (splitcnt > mapNumCons) {
//
//		double[][] nodeSeg = new double[100][3];
//
//		int sidx = kvi_rangeAdaOptPre(relregnum, extInteNode, nodepath, l, r);
//		kvi_rangeAdaOpt(relregnum + 1, extInteNode, nodeSeg, mapNumCons, sidx,
//				l, r);
//		RegAdaTableInputFormat.getRegion(mapNumCons, nodeSeg);
//
//		tmp = kvimrpro.jobAdaRegIntervalEvalQuery(l, r, idxtabname,
//				kviCol.regvalKeyCon((long) l) + ",0",
//				kviCol.regvalKeyCon((long) r + 1) + ",0", qual, runpara);
//
//		// }
//		// else
//		// {
//		// tmp = kvimrpro.jobIndiviRegIntervalEvalQuery(l, r, idxtabname,
//		// kviCol.regvalKeyCon((long) l) + ",0",
//		// kviCol.regvalKeyCon((long) r + 1) + ",0", qual, runpara);
//		// }
//
//		// .....test.........//
//
//		// System.out.printf("Interval distribution test:\n");
//		// double ta = 0.0, tb = 0.0;
//		// for (int j = 1; j < relregnum; ++j) {
//		// ta = nodeDist.bstInterCap(extInteNode[j - 1], extInteNode[j]);
//		// tb = nodeDist.bstInterNodes(extInteNode[j - 1], extInteNode[j]);
//		// System.out.printf(" %f %f", ta, tb);
//		// }
//
//		// ..................//
//
//		// ......................................................//
//
//		// tmp = kvimrpro.jobAdaRegIntervalEvalQuery(l, r, idxtabname,
//		// kviCol.regvalKeyCon((long) l) + ",0",
//		// kviCol.regvalKeyCon((long) r + 1) + ",0", qual, runpara);
//
//		edcnt = System.nanoTime();
//
//		// ......metric record............//
//		// tupRangeL = parallel[0][0];
//		// tupRangeR = parallel[1][1];
//
//		numtup = tmp;
//		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
//		inmem = inmemCnt - stcnt;
//		mracc = midcnt - inmemCnt;
//		randacc = edcnt - midcnt;
//		schlevel = resnum;
//
//		mapout = runpara[0];
//		mapcnt = runpara[1];
//
//		return true;
//	}

//	public void kvi_rangeAdaBaseline(double l, double r, String qual[], int sign)
//			throws Exception
//	// temp,
//	// 1:value {
//	{
//
//		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
//		double[] runpara = { 0.0, 0.0 };
//		double tmp = 0.0;
//		int resnum = 0;
//
//		long[] releReg = new long[200];
//		double[] extInteNode = new double[100];
//
//		stcnt = System.nanoTime();
//		int relregnum = kviCol.relAdaBaseRegIntervalSearch(l, r, releReg,
//				extInteNode);
//
//		System.out.printf("number of regions: %d \n", relregnum);
//
//		RegAdaBaseTableInputFormat.getRegion(relregnum, releReg, extInteNode,
//				relregnum, 96);
//
//		inmemCnt = System.nanoTime();
//
//		tmp = kvimrpro.jobAdaBaseRegIntervalEvalQuery(l, r, idxtabname,
//				kviCol.regvalKeyCon((long) l) + ",0",
//				kviCol.regvalKeyCon((long) r + 1) + ",0", qual, runpara);
//
//		midcnt = System.nanoTime();
//
//		edcnt = System.nanoTime();
//
//		// ......metric record............//
//		// tupRangeL = parallel[0][0];
//		// tupRangeR = parallel[1][1];
//
//		numtup = tmp;
//		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
//		inmem = inmemCnt - stcnt;
//		mracc = midcnt - inmemCnt;
//		randacc = edcnt - midcnt;
//		schlevel = resnum;
//
//		mapout = runpara[0];
//		mapcnt = runpara[1];
//
//		return;
//	}

	public boolean kvi_rangeSearchTest(double l, double r, String qual[],
			int sign)// 0: temp, 1:value
			throws Exception {

		String[] parallel = new String[5];
		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
		double[] runpara = { 0.0, 0.0 };

		// ..test..//
		double tmp = 0.0;

		stcnt = System.nanoTime();

		int resnum = kviCol.intervalSearch_resTest(parallel, l, r, sign);

		kviMRproCol.numMapIn = 0;

		inmemCnt = System.nanoTime();

		tmp = kviqueryCol.jobIntervalEvalQuery(l, r, idxtabname, parallel[0],
				parallel[1], qual, runpara);

		midcnt = System.nanoTime();

		// modification
		numtup = tmp;
		edcnt = System.nanoTime();

		// ......metric and running time record............//
		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
		inmem = inmemCnt - stcnt;
		mracc = midcnt - inmemCnt;
		randacc = edcnt - midcnt;
		schlevel = resnum;

		strk = parallel[0];
		edrk = parallel[1];
		mapout = runpara[0];
		mapcnt = runpara[1];

		return true;
	}

	public boolean kvi_rangeSearchMrkvi(double l, double r, String qual[],
			int sign)// 0: temp, 1:value
			throws Exception {

		String[] parallel = new String[5];
		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
		double[] runpara = { 0.0, 0.0 };

		// ..test..//
		double tmp = 0.0;

		stcnt = System.nanoTime();

		int resnum = kviCol.intervalSearch_resMrkvi(parallel, l, r, sign);
		kviMRproCol.numMapIn = 0;
		inmemCnt = System.nanoTime();

		// .....test...................//
		// System.out.printf("MapReduce range %s   %s\n",
		// parallel[0],parallel[1]);
		// ............................//

		tmp = kviqueryCol.jobIntervalEvalQuery(l, r, idxtabname, parallel[0],
				parallel[1], qual, runpara);

		midcnt = System.nanoTime();

		edcnt = System.nanoTime();

		// ......metric and running time record............//
		numtup = tmp;
		tduration = Math.max(midcnt - stcnt, edcnt - stcnt);
		inmem = inmemCnt - stcnt;
		mracc = midcnt - inmemCnt;
		randacc = edcnt - midcnt;
		schlevel = resnum;

		// strk = parallel[0];
		// edrk = parallel[1];
		mapout = runpara[0];
		mapcnt = runpara[1];

		return true;
	}

	// public boolean kvi_rangeSearchNoGrid(double l, double r) {
	//
	// String[][] indi = new String[100][2];
	// String[][] parallel = new String[100][2];
	// long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
	//
	// double tmp = 0.0;
	//
	// stcnt = System.nanoTime();
	//
	// int resnum = kviCol.intervalSearch_res(indi, parallel, l, r);
	//
	// inmemCnt = System.nanoTime();
	//
	// try {
	// kviMRpro.numMapIn = 0;
	// tmp = kviqueryCol.jobIntervalEvalQueryNoReducer(l, r, idxtabname,
	// parallel[0][0], parallel[1][1]);
	//
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// midcnt = System.nanoTime();
	//
	// numtup = tabscan.rangeSearch_scanNoGrid(idxtabname, indi, l, r, resnum) +
	// tmp;
	//
	// edcnt = System.nanoTime();
	//
	// tupRangeL = parallel[0][0];
	// tupRangeR = parallel[1][1];
	//
	// // ......metric and running time record............//
	// tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
	// inmem = inmemCnt - stcnt;
	// mracc = midcnt - inmemCnt;
	// randacc = edcnt - midcnt;
	// schlevel = resnum;
	//
	// return true;
	// }
	public boolean kvi_rangeSearchNoGrid(double l, double r, String qual[])
			throws Exception {

		String[][] indi = new String[100][2];
		String[][] parallel = new String[100][2];
		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;

		double tmp = 0.0;

		stcnt = System.nanoTime();

		int resnum = kviCol.intervalSearch_res(indi, parallel, l, r);

		inmemCnt = System.nanoTime();

		kviMRproCol.numMapIn = 0;
		// tmp = kviquery.jobIntervalEvalQueryNoReducer(l, r, idxtabname,
		// parallel[0][0], parallel[1][1]);

		// //...for test...//
		// System.out.print(parallel[0][0]+","+parallel[0][1]+"\n");
		// //..............//

		tmp = kviqueryCol.jobIntervalEvalQueryNoReducer(l, r, idxtabname,
				parallel[0][0], parallel[1][1], qual);

		midcnt = System.nanoTime();

		numtup = tmp;
		// numtup = tabscan.rangeSearch_scanNoGrid(idxtabname, indi, l, r,
		// resnum,qual) + tmp;

		edcnt = System.nanoTime();

		tupRangeL = parallel[0][0];
		tupRangeR = parallel[1][1];

		// ......metric and running time record............//
		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
		inmem = inmemCnt - stcnt;
		mracc = midcnt - inmemCnt;
		randacc = edcnt - midcnt;
		schlevel = resnum;

		return true;
	}

	public long kvi_composiAdaLastBit(long val) {
		return val & (-val);
	}

	public void kvi_composiAdaCost(double l, double r, String qual[], int sign,
			double res[])// 0:
			// input to mapper,
			// 1: total access tuple
			// 2: selectivity tuples estimate
			throws Exception {

		long[] releReg = new long[200];

		double[][] nodeinReg = new double[200][100];
		int[] nodeinRegCnt = new int[200];

		double[] extInteNode = new double[100];
		double[] lpNode = new double[100];
		double[] rpNode = new double[100];

		int relregnum = kviCol.relIndiviRegIntervalSearchLR(l, r, releReg,
				nodeinReg, nodeinRegCnt, extInteNode, lpNode, rpNode);

		grelregnum = relregnum;
		for (int i = 0; i < relregnum; ++i) {
			grelReg[i] = releReg[i];
			gninRegCnt[i] = nodeinRegCnt[i];

			for (int j = 0; j < nodeinRegCnt[i]; ++j) {
				gninReg[i][j] = nodeinReg[i][j];
			}
		}
		// grelReg= new double[100];
		// gninReg= new double[100][20];
		// gninRegCnt= new double[100];

		System.out.printf("number of regions: %d \n", relregnum);

		// .................adaptive cost calculation...............//

		RegTableInputFormat.getRegion(relregnum, releReg);

		RegIndiviTableInputFormat tmpformat = new RegIndiviTableInputFormat();
		int splitnum = tmpformat.calSplits();

		double wave = (double) splitnum / mapNumCons;

		// ................tuple number for mapper................//

		double[] lpathCap = new double[100];
		double[] rpathCap = new double[100];
		double seltup = 0.0;
		double innerCap = 0.0, lcap = 0.0, rcap = 0.0;
		int i = 0, lrbound = 0;
		for (i = 0; i < relregnum; i++) {
			if (extInteNode[i] <= l) {

				lpathCap[i] = nodeDist.bstNodeCap(extInteNode[i]);
				lcap += lpathCap[i];
			} else {
				lrbound = i;
				break;
			}
		}
		innerCap = nodeDist.bstInterCap(l, r);
		for (; i < relregnum; i++) {
			rpathCap[i - lrbound] = nodeDist.bstNodeCap(extInteNode[i]);
			rcap += rpathCap[i - lrbound];
		}

		double cost = lcap + innerCap + rcap;
		// if(wave>1)
		// {
		//
		//
		// }

		// double cost=0.0;
		// ................tuple number for reducer................//

		long pos = 0;
		double delta = 0.0;
		i = 0;
		double curnode = 0.0;
		while ((curnode = lpNode[i++]) != -1) {
			pos = kvi_composiAdaLastBit((long) curnode);
			delta = 1 << pos;
			seltup += (r - (curnode - delta)) / delta * lpathCap[i];
		}
		i = 0;
		while ((curnode = rpNode[i++]) != -1) {
			pos = kvi_composiAdaLastBit((long) curnode);
			delta = 1 << pos;
			seltup += (curnode + delta - l) / delta * rpathCap[i];
		}

		seltup += innerCap;
		// .......................................................//

		res[0] = wave;
		res[1] = cost;
		res[2] = seltup;

		return;
	}

//	public boolean kvi_composAdaPro(double tl, double tr, double vl, double vr,
//			String qual[], int sign)// 0:
//			// temp,
//			// 1:value
//			throws Exception {
//
//		// String[][] indi = new String[100][2];
//		// String[][] parallel = new String[100][2];
//		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
//		double[] runpara = { 0.0, 0.0 };
//		double tmp = 0.0;
//		int resnum = 0;
//
//		// long[] releReg = new long[200];
//		// String[][] scanReg = new String[200][3];
//
//		// double[][] nodeinReg = new double[200][100];
//		// int[] nodeinRegCnt = new int[200];
//
//		// double[] extInteNode = new double[100];
//		stcnt = System.nanoTime();
//
//		// int relregnum = kviCol.relIndiviRegIntervalSearch(l, r, releReg,
//		// nodeinReg, nodeinRegCnt, extInteNode);
//
//		System.out.printf("number of regions: %d \n", grelregnum);
//
//		inmemCnt = System.nanoTime();
//
//		RegIndiviTableInputFormat.getRegion(grelregnum, grelReg, gninReg,
//				gninRegCnt);
//
//		// ....................number of reducer calculation
//		// process......................//
//
//		// ...............................................................................//
//
//		if (qual[0] == "st") {
//			tmp = kvimrpro.jobComposIntervalEvalQuery(tl, tr, vl, vr,
//					idxtabname, kviCol.regvalKeyCon((long) tl) + ",0",
//					kviCol.regvalKeyCon((long) tr + 1) + ",0", qual, runpara);
//		} else {
//			tmp = kvimrpro.jobComposIntervalEvalQuery(tl, tr, vl, vr,
//					idxtabname, kviCol.regvalKeyCon((long) vl) + ",0",
//					kviCol.regvalKeyCon((long) vr + 1) + ",0", qual, runpara);
//		}
//
//		midcnt = System.nanoTime();
//
//		edcnt = System.nanoTime();
//
//		// ......metric record............//
//		// tupRangeL = parallel[0][0];
//		// tupRangeR = parallel[1][1];
//
//		numtup = tmp;
//		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
//		inmem = inmemCnt - stcnt;
//		mracc = midcnt - inmemCnt;
//		randacc = edcnt - midcnt;
//		schlevel = resnum;
//
//		mapout = runpara[0];
//		mapcnt = runpara[1];
//
//		return true;
//	}

	// .........................conventiona kvi search......................//
	// public boolean conve_kvi_rangeSearch(double l, double r) {
	// String[][] indi = new String[100][2];
	// long stcnt = 0, edcnt = 0;
	//
	// stcnt = System.nanoTime();
	// int resnum = kviCol.conve_rangeSearch_res(indi, l, r);
	// // .......metric......................//
	// numtup = tabscan.rangeSearch_scan(idxtabname, indi, l, r, resnum);
	// numtup = numtup + tabscan.rangeSearch_interRangeScan(idxtabname, l, r);
	// edcnt = System.nanoTime();
	// tduration = edcnt - stcnt;
	//
	// return true;
	// }

//	public boolean conve_kvi_rangeSearchConsec(double l, double r,
//			String qual[], int sign) {// 0: temp, 1: value
//		String[][] indi = new String[100][2];
//		long stcnt = 0, edcnt = 0, midcnt = 0;
//
//		stcnt = System.nanoTime();
//		int resnum = kviCol.conve_rangeSearch_res(indi, l, r);
//
//		numtup = 0.0;
//		// some modification
//		// if(r<=0)
//		// {
//		if (sign == 0)
//		// .......metric......................//
//		{
//			numtup = tabscan.rangeSearch_scan(idxtabname, indi, l, r, resnum,
//					qual);
//		} else {
//			resnum = 1;
//			numtup = tabscan.rangeSearch_scan(idxtabname, indi, l, r, resnum,
//					qual);
//		}
//
//		midcnt = System.nanoTime();
//		numtup = numtup
//				+ tabscan.rangeSearch_interRangeScanConsec(idxtabname, l, r,
//						qual);
//		edcnt = System.nanoTime();
//		tduration = (long) Math.max(midcnt - stcnt, (edcnt - midcnt) / 1);
//		// Math.max(midcnt - stcnt, edcnt - midcnt);
//
//		return true;
//	}

	// .....................................................................//
	public int paraOut(double para[]) {// change to kviCol class
		double[] param = new double[100];
		int num = kviCol.paraOutput(param);
		for (int i = 0; i < num; ++i) {
			para[i] = param[i];
		}
		return num;
	}

	public void paraSetup(int num, double para[]) {
		kviCol.paramConf(num, para);
		return;
	}

}
