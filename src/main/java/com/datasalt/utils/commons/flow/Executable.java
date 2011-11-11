package com.datasalt.utils.commons.flow;


public interface Executable<ConfigData> {

	public void execute(ConfigData configData) throws Exception;
	
}
