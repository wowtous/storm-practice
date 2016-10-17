package org.darebeat.dataopt.xml;

import org.darebeat.dataopt.util.MacroDef;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

/**
 * 消费者Metaq接口的配置xml读取接口
 */

public class MetaXml {

    //xml路径
	private static String fd;
    //MetaBolt参数
    //!--MetaQ消息队列--
	public static String MetaTopic;
    //!--MetaQ服务地址--
	public static String MetaZkConnect;
    //!--MetaQ服务路径--
	public static String MetaZkRoot;

	
	@SuppressWarnings("static-access")
	public MetaXml(String str){
		this.fd = str;
	}
	
	@SuppressWarnings("static-access")
	public void read(){
		try {
			File file = new File(this.fd);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);
			
			NodeList nl = doc.getElementsByTagName(MacroDef.Parameter);
			
			Element e = (Element)nl.item(0);

			MetaTopic = e.getElementsByTagName(MacroDef.MetaTopic).item(0).getFirstChild().getNodeValue();
			MetaZkConnect = e.getElementsByTagName(MacroDef.MetaZkConnect).item(0).getFirstChild().getNodeValue();
			MetaZkRoot = e.getElementsByTagName(MacroDef.MetaZkRoot).item(0).getFirstChild().getNodeValue();
			
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
