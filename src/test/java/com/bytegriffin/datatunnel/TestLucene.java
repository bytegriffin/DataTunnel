package com.bytegriffin.datatunnel;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class TestLucene {

	private static final Analyzer analyzer = new SmartChineseAnalyzer(); // 中文分词
	private static final String dir = "/opt/Persons";

	private static FSDirectory getFSDirectory() {
		try {
			return FSDirectory.open(Paths.get(dir));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 插入数据
	 */
   static void insertData() {
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(OpenMode.CREATE);
        LogDocMergePolicy policy = new LogDocMergePolicy();
        policy.setMergeFactor(20);
        config.setMergePolicy(policy);
        config.setRAMBufferSizeMB(100);
        config.setUseCompoundFile(false);
        try {
			@SuppressWarnings("resource")
			IndexWriter indexWriter = new IndexWriter(getFSDirectory(), config);
			Document doc = new Document();
			doc.add(new StringField("field_name", "test", Field.Store.YES));
			indexWriter.addDocument(doc);
           // indexWriter.updateDocument(new Term("id", "abc"), doc);
            indexWriter.forceMergeDeletes();
            indexWriter.commit();
		} catch (IOException e) {
			e.printStackTrace();
		}   
	}
	
	/**
	 * 更新数据
	 */
	static void updateData() {
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(OpenMode.CREATE_OR_APPEND);
        LogDocMergePolicy policy = new LogDocMergePolicy();
        policy.setMergeFactor(20);
        config.setMergePolicy(policy);
        config.setRAMBufferSizeMB(100);
        config.setUseCompoundFile(false);
        try {
			@SuppressWarnings("resource")
			IndexWriter indexWriter = new IndexWriter(getFSDirectory(), config);
			Document doc = new Document();
			doc.add(new StringField("field_name", "test", Field.Store.YES));
            indexWriter.updateDocument(new Term("id", "abc"), doc);
            indexWriter.forceMergeDeletes();
            indexWriter.commit();
		} catch (IOException e) {
			e.printStackTrace();
		}   
	}

	/**
	 * 删除数据
	 */
    static void deleteData() {
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(OpenMode.CREATE_OR_APPEND);
        LogDocMergePolicy policy = new LogDocMergePolicy();
        policy.setMergeFactor(20);
        config.setMergePolicy(policy);
        config.setRAMBufferSizeMB(100);
        config.setUseCompoundFile(false);
        try {
			@SuppressWarnings("resource")
			IndexWriter indexWriter = new IndexWriter(getFSDirectory(), config);
			Document doc = new Document();
			doc.add(new StringField("field_name", "test", Field.Store.YES));
            indexWriter.deleteDocuments(new Term("id", "abc"));
            indexWriter.forceMergeDeletes();
            indexWriter.commit();
		} catch (IOException e) {
			e.printStackTrace();
		}   
	}

	/**
	 * 查询数据
	 */
	static void select() {
        try (IndexReader indexReader = DirectoryReader.open(getFSDirectory())){
			IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		    TermQuery query = new TermQuery(new Term("LastName","ffff"));
		    TopDocs topDocs = indexSearcher.search(query, 5);
		    ScoreDoc[] scoreDocs = topDocs.scoreDocs;
		    for (ScoreDoc scoreDoc : scoreDocs) {
		    	int docID = scoreDoc.doc;
	            Document document = indexSearcher.doc(docID);
	            System.out.println(document.get("Address"));
		    }
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		select();
    }

}
