package part4;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import part4.HierarchyXml;

class MovieTagHierarchyReducer extends Reducer <Text, Text, Text, NullWritable>{
	
	private ArrayList<String> tags = new ArrayList<String>();
	private static final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	private String movie = null;
	private String memoizedToString = null;
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		movie = null;
		tags.clear();
		
		for(Text t : values) {
			if(t.charAt(0) == 'M') {
				movie = t.toString().substring(1, t.toString().length()).trim();
				System.out.println("!" + movie + "!");
			}
			else if(t.charAt(0) == 'T'){
				tags.add(t.toString().substring(1, t.toString().length()).trim());
				//System.out.println("!" + tags + "!");
			}
		}
		
		if(movie != null) {
			HierarchyXml doc;
			try {
				
				doc = new HierarchyXml(movie, tags);
				
				//context.write(new Text(doc.toString()), NullWritable.get());
				context.write(new Text(movie + ":" + tags), NullWritable.get());
			
			} catch (Exception e) {				
				e.printStackTrace();
			}
			
			
		}
	}
	
	private String nestElements(String movie, ArrayList<String> tags)  
			throws ParserConfigurationException, SAXException, IOException {
		
			DocumentBuilder bldr = dbf.newDocumentBuilder();
			Document doc = bldr.newDocument();
			
			Element movieEle = getXmlElementFromString(movie);
			Element toAddMovieEle = doc.createElement("movie");
			
			copyAttributesToElement(movieEle.getAttributes(), toAddMovieEle);
			
			for(String tagXml : tags) {
				
				Element tagEle = getXmlElementFromString(tagXml);
				Element toAddTagEle = doc.createElement("tags");
				
				copyAttributesToElement(tagEle.getAttributes(), toAddTagEle);
				toAddMovieEle.appendChild(toAddTagEle);		
			}
			doc.appendChild(toAddMovieEle);
			
			
		return transformDocumentToString(doc);
	}

	private Element getXmlElementFromString(String xml) throws ParserConfigurationException, SAXException, IOException {
		
		DocumentBuilder builder = dbf.newDocumentBuilder();
		
		return builder.parse(new InputSource(new StringReader(xml)))
						.getDocumentElement();

	}
	
	private void copyAttributesToElement(NamedNodeMap attributes, Element element) {
		for (int i = 0; i < attributes.getLength(); ++i) {
			Attr toCopy = (Attr) attributes.item(i);
			element.setAttribute(toCopy.getName(), toCopy.getValue());
		}
	}
	
	private String transformDocumentToString(Document doc) {
		
		
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
