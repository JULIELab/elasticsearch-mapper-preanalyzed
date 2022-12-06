package org.elasticsearch.index.mapper.preanalyzed;


import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.XContentLocation;

import java.util.function.Supplier;

public class NoopDeprecationHandler implements DeprecationHandler {

	@Override
	public void usedDeprecatedName(String s, Supplier<XContentLocation> supplier, String s1, String s2) {

	}

	@Override
	public void usedDeprecatedField(String s, Supplier<XContentLocation> supplier, String s1, String s2) {

	}

	@Override
	public void usedDeprecatedField(String s, Supplier<XContentLocation> supplier, String s1) {

	}
}
