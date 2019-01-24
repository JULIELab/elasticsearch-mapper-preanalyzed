package org.elasticsearch.index.mapper.preanalyzed;

import org.elasticsearch.common.xcontent.DeprecationHandler;

public class NoopDeprecationHandler implements DeprecationHandler {

	@Override
	public void usedDeprecatedName(String usedName, String modernName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void usedDeprecatedField(String usedName, String replacedWith) {
		// TODO Auto-generated method stub

	}

}
