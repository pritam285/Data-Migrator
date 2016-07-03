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

package com.yukthi.ccg.xml;

/**
 * XML Parser plugin for handling custom nodes. 
 * @author akiran
 */
public interface ICustomNodeHandler
{
	/**
	 * Called when a new custom node is encountered (with non default name). This method is expected to return
	 * bean, if the node can be handled.
	 * @param node Custom node started
	 * @param att Attributes of custom node.
	 * @return Bean representing custom node.
	 */
	public Object createCustomNodeBean(BeanNode node, XMLAttributeMap att);
	
	/**
	 * Called when custom node is encountered.
	 * @param node Node ended.
	 * @param att Attributes of the node.
	 */
	public void handleCustomNodeEnd(BeanNode node, XMLAttributeMap att);
}
