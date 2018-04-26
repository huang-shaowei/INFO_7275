package part4;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

class HierarchyXml {
	private static final DocumentBuilderFactory dbf = DocumentBuilderFactory
					.newInstance();
	private final Document doc;
	
	public HierarchyXml(String movie, List<String> tags) 
				throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilder builder = dbf.newDocumentBuilder();
		doc = builder.newDocument();
		
		Element movieEl = getXmlElementFromString(movie);
		Element toAddMovieEl = doc.createElement("movie");
		
		copyAttributesToElement(movieEl.getAttributes(), toAddMovieEl);
		
		for(String tagXml : tags) {
			Element tagEl = getXmlElementFromString(tagXml);
			Element toAddTagEl = doc.createElement("tags");
			
			copyAttributesToElement(tagEl.getAttributes(), toAddTagEl);
			toAddMovieEl.appendChild(toAddTagEl);
		}
		
		doc.appendChild(toAddMovieEl);
	}
	
	public static Element getXmlElementFromString(String xml) 
					throws ParserConfigurationException, SAXException, IOException{
		DocumentBuilder builder = dbf.newDocumentBuilder();
		
		return builder.parse(new InputSource(new StringReader(xml)))
						.getDocumentElement();
	}
	
	private void copyAttributesToElement(NamedNodeMap attributes, Element element) {
		for(int i = 0; i < attributes.getLength(); ++i) {
			Attr toCopy = (Attr) attributes.item(i);
			element.setAttribute(toCopy.getName(), toCopy.getValue());
		}
	}
	
	private String memoizedToString = null;
	
	@Override
	public String toString() {
		
		if(memoizedToString != null) {
			return memoizedToString;
		}
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer;
		try {
				transformer = tf.newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
							"yes");
				StringWriter writer = new StringWriter();
				transformer.transform(new DOMSource(doc), new StreamResult(writer));
				
				memoizedToString = writer.getBuffer().toString()
									.replaceAll("\n|\r", "");
		} catch (Exception e) {
			e.printStackTrace();
			memoizedToString = "";
		}
		
		return memoizedToString;
	}
	
}
