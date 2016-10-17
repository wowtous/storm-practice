package org.darebeat.dataopt.util.SpoutMetaq;

import com.taobao.metamorphosis.Message;

import java.util.concurrent.CountDownLatch;

/**
 * Meta消息封装
 */

public final class MetaMessageWrapper {

	public final Message message;
	public final CountDownLatch latch;
	public volatile boolean success = false;

	public MetaMessageWrapper(final Message message) {
		super();
		this.message = message;
		this.latch = new CountDownLatch(1);
	}
}