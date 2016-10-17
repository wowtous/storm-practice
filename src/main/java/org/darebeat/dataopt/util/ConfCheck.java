package org.darebeat.dataopt.util;

import org.darebeat.dataopt.bolt.FilterBolt;
import org.darebeat.dataopt.bolt.MetaBolt;
import org.darebeat.dataopt.bolt.MysqlBolt;
import org.darebeat.dataopt.spout.MetaSpout;

import java.io.File;

/**
 * 对配置文件进行动态监测
 * 该函数定时检测配置文件是否发生改变，
 * 实现了Topology的动态配置 即在不重启top的情况下，实现数据处理的动态配置
 */

public class ConfCheck extends Thread {

	private String xmlpath = "xxx.xml";
	private int heartbeat = 1000;
	private String type = "type";

	public ConfCheck(String XmlPath, int HeartBeat, String type) {
		this.xmlpath = XmlPath;
		this.heartbeat = HeartBeat;
		this.type = type;
	}

	public void run() {

        // hash初始值
        long init_time = 0;

		for (int i = 0;; i++) {

			try {

				File file = new File(this.xmlpath);
                // 检查时间戳是否一致
				long lasttime = file.lastModified();

				if (i == 0) {
					init_time = lasttime;
				} else {
					if (init_time != lasttime) {

						init_time = lasttime;

						if (this.type.equals(MacroDef.Thread_type_metaqspout)) {
							MetaSpout.isload();
						} else if (this.type
								.equals(MacroDef.Thread_type_filterbolt)) {
							FilterBolt.isload();
						} else if (this.type
								.equals(MacroDef.Thread_type_mysqlbolt)) {
							MysqlBolt.isload();
						} else if (this.type
								.equals(MacroDef.Thread_type_metaqbolt)) {
							MetaBolt.isload();
						}
					}
				}

				Thread.sleep(this.heartbeat);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		new ConfCheck("MysqlBolt.xml", 1000, "test").start();
		Thread.sleep(100000);
	}

}
