package org.darebeat.click.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.darebeat.click.common.FieldNames;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class GeoStatsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 8918144184085398047L;

	private class CountryStats {
		private int countryTotal = 0;
		private static final int COUNT_INDEX = 0;
		private static final int PERCENTAGE_INDEX = 1;
		private String countryName;
		private Map<String, List<Integer>> cityStats =
				new HashMap<>();
		
		public CountryStats(String countryName) {
			this.countryName = countryName;
		}
		
		public void cityFound(String cityName) {
			countryTotal ++;
			if ( cityStats.containsKey(cityName) ) {
				cityStats.get(cityName).set(COUNT_INDEX, 
						cityStats.get(cityName)
						.get(COUNT_INDEX).intValue() + 1);
			}
			else {
				List<Integer> list = new LinkedList<>();
				list.add(1);
				list.add(0);
				cityStats.put(cityName, list);
			}
			
			double percent = (double)cityStats
					.get(cityName).get(COUNT_INDEX).intValue() / countryTotal;
			cityStats.get(cityName).set(PERCENTAGE_INDEX, 
					(int)percent);
		}
		
		public int getCountryTotal() {
			return countryTotal;
		}
		
		public int getCityTotal(String cityName) {
			return cityStats.get(cityName).
					get(COUNT_INDEX).intValue();
		}
		
		public String toString() {
			return "Total Count for " + countryName +
					" is " + Integer.toString(countryTotal) +
					"\nCities: " + cityStats.toString();
		}
	}
	
	private OutputCollector outputCollector;
	private Map<String, CountryStats> stats = new HashMap<>();
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext,
			OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		String country = tuple.getStringByField(FieldNames.COUNTRY);
		String city = tuple.getStringByField(FieldNames.CITY);
		if ( !stats.containsKey(country) ) {
			stats.put(country, new CountryStats(country));
		}
		stats.get(country).cityFound(city);
		outputCollector.emit(
				new Values(country,
						stats.get(country).getCountryTotal(),
						city,
						stats.get(country).getCityTotal(city))
		);
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.COUNTRY,
				FieldNames.COUNTRY_TOTAL,
				FieldNames.CITY,
				FieldNames.CITY_TOTAL
		));
	}

}
