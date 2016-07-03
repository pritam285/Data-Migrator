/*
 * The MIT License (MIT)
 * Copyright (c) 2016 "Yukthi Techsoft Pvt. Ltd." (http://yukthi-tech.co.in)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.yukthi.test;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yukthi.ccg.xml.XMLBeanParser;

/**
 * @author akiran
 *
 */
public class TXmlBeanParser
{
	@Test
	public void testDynamicBean()
	{
		DynamicTestBean bean = new DynamicTestBean();
		XMLBeanParser.parse(TXmlBeanParser.class.getResourceAsStream("/xml-parser-dynamic-bean.xml"), bean);
		
		Assert.assertEquals(bean.getData().get("attr1"), "val1");
		Assert.assertEquals(bean.getData().get("attr2"), "val2");
		Assert.assertEquals(bean.getData().get("node1"), "node-val1");
		Assert.assertEquals(bean.getData().get("node2"), "node-val2");
		
		TestDataBean testDataBean = (TestDataBean)bean.getData().get("node3");
		Assert.assertEquals(testDataBean.getIntVal(), 100);
	}
}
