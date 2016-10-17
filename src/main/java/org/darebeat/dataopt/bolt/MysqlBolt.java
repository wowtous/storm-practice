package org.darebeat.dataopt.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.darebeat.dataopt.util.ConfCheck;
import org.darebeat.dataopt.util.MacroDef;
import org.darebeat.dataopt.util.MysqlOpt;
import org.darebeat.dataopt.xml.MysqlXml;

import java.util.Map;

/**
 * 数据落地Mysql接口
 */

@SuppressWarnings("serial")
public class MysqlBolt implements IRichBolt {

	@SuppressWarnings("unused")
	private OutputCollector collector;

    //是否加载配置标志位
	private static boolean flag_load = false;
	
	private long register = 0;

	String mysqlXml = "Mysql.xml";

	MysqlOpt mysql = new MysqlOpt();

	private boolean flag_par = true;
	private boolean flag_xml = true;

	String from = "monitor"; // 表名

    // 构造函数
	public MysqlBolt(String MysqlXML) {
		if (MysqlXML == null) {
			flag_par = false;
		} else {
			this.mysqlXml = MysqlXML;
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		System.out.println("MysqlBolt	--	Start!");
		this.collector = collector;
		
		if (!this.flag_par) {
			System.out.println("MetaSpout-- Erre: can't get the path of Spout.xml!");
		} else {
            // 调用检测线程
			new ConfCheck(this.mysqlXml, MacroDef.HEART_BEAT,MacroDef.Thread_type_mysqlbolt).start();
		}
	}

    // 更改标志位
	public static void isload() {
		flag_load = false;
	}

    // 参数初始化
	public void Loading() {

		new MysqlXml(this.mysqlXml).read();
        // mysql地址及端口
		String host_port = MysqlXml.Host_port;
        // 数据库名
		String database = MysqlXml.Database;
        // 用户名
		String username = MysqlXml.Username;
        // 密码
		String password = MysqlXml.Password;
        // 表名
		this.from = MysqlXml.From;

		if (!this.mysql.connSQL(host_port, database, username, password)) {
			System.out.println("MysqlBolt--Config errer, Please check Mysql-conf: " + this.mysqlXml);
			flag_xml = false;
		} else {
			System.out.println("MysqlBolt-- test connect mysql success: " + this.mysqlXml);
		}
        flag_load = true;
	}

	@Override
	public void execute(Tuple input) {

		String str = input.getString(0);

		if (!this.flag_par) {
			System.out.println("MysqlBolt-- Erre: can't get the path of Mysql.xml!");
		} else {

            // 检测配置文件是否更改
			if (!flag_load ) {
                // 配置文件发生更改则进行加载参数操作
				Loading();
				if (register != 0) {
					System.out.println("MysqlBolt-- Conf Change: " + this.mysqlXml);
				} else {
					System.out.println("MysqlBolt-- Conf Loaded: " + this.mysqlXml);
				}
			}

			if (this.flag_xml) {

				String sql = send_str(str);

				if (!this.mysql.insertSQL(sql)) {
					System.out.println("MysqlBolt-- Erre: can't insert tuple into database!");
					System.out.println("MysqlBolt-- Error Tuple: " + str);
					System.out.println("SQL: " + sql);
				}
			}
		}

	}

	public String send_str(String str) {

		String send_tmp = null;
		String field[] = str.split(MacroDef.FLAG_TABS);

		for (int i = 0; i < field.length; i++) {

			if (i == 0) {
				send_tmp = "'" + field[0] + "', '";
			} else if (i == (field.length - 1)) {
				send_tmp = send_tmp + field[i] + "'";
			} else {
				send_tmp = send_tmp + field[i] + "', '";
			}
		}
		String send = "insert into " + this.from + "(domain, value, time, validity, seller) values (" + send_tmp + ");";

		return send;
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
