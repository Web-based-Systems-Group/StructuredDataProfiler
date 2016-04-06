package org.webdatacommons.structureddata;

import org.webdatacommons.structureddata.stats.CCUrlStatsCalculator;
import org.webdatacommons.structureddata.stats.WDCQuadStatsCalculator;
import org.webdatacommons.structureddata.stats.WDCUrlStatsCalculator;
import org.webdatacommons.structureddata.util.QuadSorter;

import com.beust.jcommander.JCommander;

public class Master {

	public static void main(String[] args) {
		// init
		Master master = new Master();
		JCommander jc = new JCommander(master);

		// add URL Stats
		CCUrlStatsCalculator ccurls = new CCUrlStatsCalculator();
		jc.addCommand("ccurlstats", ccurls);

		WDCUrlStatsCalculator wdcurls = new WDCUrlStatsCalculator();
		jc.addCommand("wdcurlstats", wdcurls);

		WDCQuadStatsCalculator wdcquads = new WDCQuadStatsCalculator();
		jc.addCommand("wdcquadstats", wdcquads);

		QuadSorter sort = new QuadSorter();
		jc.addCommand("sortquads", sort);

		try {
			jc.parse(args);
			switch (jc.getParsedCommand()) {
			case "ccurlstats":
				ccurls.process();
				break;
			case "wdcurlstats":
				wdcurls.process();
				break;
			case "wdcquadstats":
				wdcquads.process();
				break;
			case "sortquads":
				sort.process();
				break;
			}
		} catch (Exception pex) {
			if (jc.getParsedCommand() == null) {
				jc.usage();
			} else {
				switch (jc.getParsedCommand()) {
				case "ccurlstats":
					new JCommander(ccurls).usage();
					break;
				case "wdcurlstats":
					new JCommander(wdcurls).usage();
					break;
				case "wdcquadstats":
					new JCommander(wdcquads).usage();
					break;
				case "sortquads":
					new JCommander(sort).usage();
					break;
				default:
					jc.usage();
				}
			}
		}

	}

}
