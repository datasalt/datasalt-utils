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

import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Utils for general URL tasks.
 * @author jaylinux
 *
 */
public class URLUtils {
	static Logger log = Logger.getLogger(URLUtils.class);
	public static String getBase(String url)
	{
		return StringUtils.substringBefore(url,"?");
	}
	/**
	 * build a url from a hashtable and a base.  Not tested but may be useful for building urls from 
	 * other data.  Also might be usefull for testing later on .
	 * @param base
	 * @param pars
	 * @return
	 */
	@SuppressWarnings("rawtypes")
  public static String getUrlFromParameters(String base, Map pars)
	{
		StringBuffer s = new StringBuffer(base);
		if(pars.size()>0)
			s.append("?");
		for(Object k : pars.keySet())
		{
			s.append(k);
			s.append("=");
			s.append(pars.get(k));
		}
		return s.toString();
	}
	/**
	   Extract parameters from a url so that Ning (or other crawling utilities) 
	   can properly encode them if necessary. This was necesssary for facebook requests, 
	   which are bundled with "next urls" that have funny characters in them such as this 
	   
	   "122524694445860|7fc6dd5fe13b43c09dad009d.1-1056745212|An3Xub_HEDRsGxVPkmy71VdkFhQ"
	 * @param url
	 * @return
	 */
	public static Hashtable<String,String> extractParameters(String url)
	{
		Hashtable<String,String> pars = new Hashtable<String,String>();
		if(! url.contains("?"))
		{
			log.warn("WARNING : URL HAS NO PARAMETERS ! " + url);
			return pars;
		}
		String parameters = StringUtils.substringAfter(url,"?");
		for(String pairs : parameters.split("&"))
		{	
			String[] nv=pairs.split("=");
			pars.put(nv[0],nv[1]);
		}
		return pars;
	}

}
