package org.darebeat.dataopt.util.SpoutMetaq;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.darebeat.dataopt.util.MacroDef;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Meta消息封装
 */

//规范化输出，定义编码
public class StringScheme implements Scheme {

	public List<Object> deserialize(byte[] bytes) {

		try {

            //数据的编码定义
			return new Values(new String(bytes, MacroDef.ENCODING));

		} catch (UnsupportedEncodingException e) {

			throw new RuntimeException(e);

		}
	}

	@Override
	public List<Object> deserialize(ByteBuffer byteBuffer) {
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("str");
	}
}