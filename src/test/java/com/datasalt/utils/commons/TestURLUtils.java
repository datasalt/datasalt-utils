/**
 * Copyright [2011] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasalt.utils.commons;

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;


import org.junit.Test;

import com.datasalt.utils.commons.URLUtils;
import com.datasalt.utils.commons.test.BaseTest;

public class TestURLUtils extends BaseTest{

	@Test
	public void testUrlUtils() throws IOException
	{
		String access_token="access_token";
		String value = "122524694445860|7fc6dd5fe13b43c09dad009d.1-1056745212|An3Xub_HEDRsGxVPkmy71VdkFhQ";
		String url = "https://graph.facebook.com/me/friends?";
		url=url+"access_token";
		url=url+"=";
		url=url+value;
		url=url+"&a";
		url=url+"=1";
		Map m = URLUtils.extractParameters(url);
		System.out.println(m);
		
		//We should see 2 pars in the url : "access_token" and "a" 
		Assert.assertEquals(2, m.size());
		
		//get the acess token value
		String value2 = m.get(access_token).toString();

		//it should be the same as the "value" variable. 
		Assert.assertEquals(value2.length(), value.length());
		Assert.assertEquals(value2, value);
		
	}
}
