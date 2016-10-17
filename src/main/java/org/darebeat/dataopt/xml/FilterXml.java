package org.darebeat.dataopt.xml;

import org.darebeat.dataopt.util.MacroDef;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

/**
 * 数据过滤Filter接口的xml配置读取接口
 */

public class FilterXml {

    // xml路径
	private String fd = null;
    // MetaBolt参数
    // 匹配条件间的逻辑关系
	public static String MatchLogic;
    // !--匹配类型列表
	public static String MatchType;
    // !--匹配字段列表-
	public static String MatchField;
    // !--字段值列表-
	public static String FieldValue;

	public FilterXml(String str) {
		this.fd = str;
	}

	public void read() {
		try {
			File file = new File(this.fd);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);

			NodeList nl = doc.getElementsByTagName(MacroDef.Parameter);

			Element e = (Element) nl.item(0);

			MatchLogic = e.getElementsByTagName(MacroDef.MatchLogic).item(0)
					.getFirstChild().getNodeValue();
			MatchType = e.getElementsByTagName(MacroDef.MatchType).item(0)
					.getFirstChild().getNodeValue();
			MatchField = e.getElementsByTagName(MacroDef.MatchField).item(0)
					.getFirstChild().getNodeValue();
			FieldValue = e.getElementsByTagName(MacroDef.FieldValue).item(0)
					.getFirstChild().getNodeValue();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
