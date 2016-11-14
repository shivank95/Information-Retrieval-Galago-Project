import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.utility.ByteUtil;

/*
 * Author: Shivank Tibrewal
 * CS 473 Project 3
 * Nov 9th 2016
 */


public class IndexStatistic {

	public static String indexPath = "/Users/shivanktibrewal/Desktop/galago-3.10/wiki-small-index";
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
		
		//Map<String,String> myParameters = new HashMap<String,String>();
		//myParameters.put("--iidList", "1,5,10");
		
		//Parameters myParams = Parameters.create();
		//myParams.parseMap(myParameters);
		
        Retrieval retrieval = RetrievalFactory.instance(indexPath, Parameters.create());
        //Retrieval retrieval = RetrievalFactory.instance(indexPath, myParams);
        
        Node n = new Node();
        n.setOperator("lengths");
        n.getNodeParameters().set("part", "lengths");

        FieldStatistics stat = retrieval.getCollectionStatistics(n);

        long maxLength = stat.maxLength;
        long minLength = stat.minLength;
        double avgLength = stat.avgLength;
        long documentCount = stat.documentCount;
        long firstDocId = stat.firstDocId;
        long lastDocId = stat.lastDocId;
        
        long collectionLength = stat.collectionLength;

        //System.out.printf ("Min length       : %d \n", minLength);
        System.out.printf ("Total # of Documents   : %d \n", documentCount);
        System.out.printf ("Average length of Documents   : %f \n", avgLength);
        
        String index = "postings";
	    IndexPartStatistics stat2 = retrieval.getIndexPartStatistics(index);
	    
	    long vocabcount = stat2.vocabCount; 
	    System.out.printf("# of Unique Words in whole Corpus	: %d \n", vocabcount);
        
        //System.out.println("# of Unique Words in the whole Corpus: " + collectionLength);
        //System.out.printf ("Max Document length       : %d \n", maxLength);

		
        printWordStatistics("maximum");
        printWordStatistics("entropy");
        printWordStatistics("data");
        printWordStatistics("science");

        findAndPrintLongestDocumentInfo(retrieval, maxLength, stat);
        
        printMaxOccDoc("entropy");
        printMaxOccDoc("science");

        String[] terms2 = {"maximum","entropy","data","science"};
        booleanAND(terms2);
        
        String[] terms = {"maximum", "entropy"};
        booleanAND(terms);
        
        
        
	}
	
	private static long getDocumentLen(long maxDocID) throws Exception{

		Retrieval retrieval = RetrievalFactory.instance(indexPath, Parameters.create());
		
		Node n = new Node();
		n.setOperator("lengths");
		n.getNodeParameters().set("part", "lengths");

		String docno = retrieval.getDocumentName( (int) maxDocID );
		long docLength = retrieval.getDocumentLength(docno);
		return docLength;
	}
	
	private static double calculateTermFreq(String term) throws Exception {
		
		File pathPosting = new File( new File( indexPath ), "postings");
		
		DiskIndex index = new DiskIndex( indexPath );
		IndexPartReader posting = DiskIndex.openIndexPart( pathPosting.getAbsolutePath() );
		KeyIterator vocabulary = posting.getIterator();

		if ( vocabulary.skipToKey( ByteUtil.fromString( term ) ) && term.equals( vocabulary.getKeyString() ) ) {

			CountIterator iterator = (CountIterator) vocabulary.getValueIterator();
			ScoringContext sc = new ScoringContext();

			int maxFrequency = 0;
			String maxDocName = "";
			long maxDocID = 0;

			while ( !iterator.isDone() ) {
				sc.document = iterator.currentCandidate();
				int freq = iterator.count( sc ); 
				String docno = index.getName( sc.document ); 

				if (freq > maxFrequency) {
					maxFrequency = freq;
					maxDocName = docno;
					maxDocID = sc.document;
				}
				iterator.movePast( iterator.currentCandidate() ); 
			}

			long doclength = getDocumentLen(maxDocID);
			double TF = (double) maxFrequency / (double)doclength; 
			System.out.println("TF: " + TF);
			return TF;
		}
		return 0;
	}
	
	private static void printMaxOccDoc(String term) throws Exception {
		
		//String  field= "html";
		String pathIndex = "/Users/shivanktibrewal/Desktop/galago-3.10/wiki-small-index";
		
		File pathPosting = new File( new File( pathIndex ), "postings" );
		
		DiskIndex index = new DiskIndex( pathIndex );
		IndexPartReader posting = DiskIndex.openIndexPart( pathPosting.getAbsolutePath() );
		
		//System.out.printf( "%-10s%-15s%-10s\n", "DOCID", "FREQ", "DOCPATH" );
		
		KeyIterator vocabulary = posting.getIterator();
		// try to locate the term in the vocabulary
		if ( vocabulary.skipToKey( ByteUtil.fromString( term ) ) && term.equals( vocabulary.getKeyString() ) ) {
			// get an iterator for the term's posting list
			CountIterator iterator = (CountIterator) vocabulary.getValueIterator();
			ScoringContext sc = new ScoringContext();
			
			int maxFreq = 0;
			String maxDocName = "";
			long maxDocID = 0;
			
			while ( !iterator.isDone() ) {
				// Get the current entry's document id.
				// Note that you need to assign the value of sc.document,
				// otherwise count(sc) and others will not work correctly.
				sc.document = iterator.currentCandidate();
				int freq = iterator.count( sc ); // get the frequency of the term in the current document
				String docno = index.getName( sc.document ); // get the docno (external ID) of the current document
				
				//System.out.printf( "%-10s%-15s%-10s\n", sc.document, freq, docno );
				if (freq > maxFreq) {
					maxFreq = freq;
					maxDocName = docno;
					maxDocID = sc.document;
				}
				
				iterator.movePast( iterator.currentCandidate() ); // jump to the entry right after the current one
			}
			
			System.out.println("\n\nMAX OCCURRENCE INFO FOR: '" + term + "'");
			System.out.println("DOCNO: " + maxDocID + "\nFrequency: " + maxFreq + "\nDOCPATH: " + maxDocName);
			
		}
		
		posting.close();
		index.close();
		
	}
	
	private static void findAndPrintLongestDocumentInfo(Retrieval retrieval, long maxLength, FieldStatistics stat) throws Exception {
		
		Set<String> docnos = new TreeSet<>();
        
        
        // Now we iteratively read and print out the internal and external IDs for documents in the index.
		for ( long docid = stat.firstDocId; docid <= stat.lastDocId; docid++ ) {
			String docno = retrieval.getDocumentName( (int) docid );
			
			
			long docLength = retrieval.getDocumentLength(docno);
			
			if (docLength == maxLength) {
				System.out.println("\n\nLongest Document Info: \nDOCNO: " + docid + " \nDOCPATH: " + docno + "\nDOC LENGTH: " + docLength);
			}
			
		}
		
	}
	
	private static Map<String,String> getDocumentList(String term) throws Exception {
		
		//String field = "html";
		
		String pathIndex = "/Users/shivanktibrewal/Desktop/galago-3.10/wiki-small-index";
		
		File pathPosting = new File( new File( pathIndex ), "postings" );
		
		DiskIndex index = new DiskIndex( pathIndex );
		IndexPartReader posting = DiskIndex.openIndexPart( pathPosting.getAbsolutePath() );
		
		
		Map<String,String> postingList = new HashMap<String,String>();
		
		KeyIterator vocabulary = posting.getIterator();
		// try to locate the term in the vocabulary
		if ( vocabulary.skipToKey( ByteUtil.fromString( term ) ) && term.equals( vocabulary.getKeyString() ) ) {
			// get an iterator for the term's posting list
			CountIterator iterator = (CountIterator) vocabulary.getValueIterator();
			ScoringContext sc = new ScoringContext();
			
			//System.out.println("\n\nPOSTING LIST FOR '" + term + "'");
			
			while ( !iterator.isDone() ) {
				// Get the current entry's document id.
				// Note that you need to assign the value of sc.document,
				// otherwise count(sc) and others will not work correctly.
				sc.document = iterator.currentCandidate();
				int freq = iterator.count( sc ); // get the frequency of the term in the current document
				String docno = index.getName( sc.document ); // get the docno (external ID) of the current document
				
				//System.out.printf( "%-10s%-15s%-10s\n", sc.document, freq, docno );
				
				postingList.put(Long.toString(sc.document), docno);
				
				iterator.movePast( iterator.currentCandidate() ); // jump to the entry right after the current one
			}
			
		}
		
		posting.close();
		index.close();
		
		
		return postingList;
		
	}
	
	//Boolean AND for any number of singular terms
	private static void booleanAND(String[] terms) throws Exception {
		
		if (terms.length == 0) {
			return;
		}
		
		ArrayList<Map<String,String>> mapList = new ArrayList<Map<String,String>>();
		
		for (int i = 0; i < terms.length; i++) {
			Map<String,String> currentMap = getDocumentList(terms[i]); 
			mapList.add(currentMap);
		}
		
		Iterator it = mapList.get(0).entrySet().iterator();
		System.out.println("\n\nBOOLEAN-AND DOC LISTING FOR : " + Arrays.toString(terms));
		
		while (it.hasNext()) {
			
			Map.Entry<String, String> pair = (Map.Entry<String, String>)it.next();
			
			boolean check = true;
			
			for (int i = 1; i < mapList.size(); i++) {
				Map<String,String> currentMap = mapList.get(i);
				
				if (!currentMap.containsKey(pair.getKey())) {
					check = false;
					break;
				}
				
			}
			
			if (check == true) {
				System.out.println(pair.getKey() + " " + pair.getValue());
			}
			it.remove();
			
		}
		
		
		
	}
	
	//Code Snippet from https://github.com/jiepujiang/cs646_tutorials/blob/master/src/main/java/edu/umass/cs/cs646/tutorial/galago/GalagoCorpusStats.java
	private static void printWordStatistics(String term) throws Exception {
		
		//String field = "html";
		Retrieval retrieval = RetrievalFactory.instance( indexPath );
		
		Node termNode = StructuredQuery.parse( term );
		termNode.getNodeParameters().set( "queryType", "count" );
		termNode = retrieval.transformQuery(termNode, Parameters.create());
		
		NodeStatistics termStats = retrieval.getNodeStatistics( termNode );
		long corpusTF = termStats.nodeFrequency; // Get the total frequency of the term in the html field
		long n = termStats.nodeDocumentCount; // Get the document frequency (DF) of the term (only counting the html field)
		
		
		//Maximum Count For each term
		System.out.println("Max Count in one document: " + termStats.maximumCount);
		
		
		Node fieldNode = new Node();
		fieldNode.setOperator("lengths");
		fieldNode.getNodeParameters().set("part","lengths");
		
		FieldStatistics fieldStats = retrieval.getCollectionStatistics( fieldNode );
		long corpusLength = fieldStats.collectionLength; // Get the length of the corpus (only counting the html field)
		long N = fieldStats.documentCount; // Get the total number of documents
		
		
		double idf = Math.log( ( N + 1 ) / ( n + 1 ) ); // Normalize N,n by adding 1 to avoid n = 0
		double pwc = 1.0 * corpusTF / corpusLength;
		
		System.out.printf( "%-30sN=%-10dn=%-10dIDF=%-8.2f\n", term, N, n, idf );
		System.out.printf( "%-30slen(corpus)=%-10dTF(%s)=%-10dP(%s|corpus)=%-10.6f\n", term, corpusLength, term, corpusTF, term, pwc );
		
		double tf = calculateTermFreq(term);
		double tf_idf = idf*tf;
		System.out.println("TF*IDF: " + tf_idf + "\n\n");
		
		
		retrieval.close();
		
	}
	
}
