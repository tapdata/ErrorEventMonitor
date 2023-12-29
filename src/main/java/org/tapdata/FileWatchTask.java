package org.tapdata;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileWatchTask implements Runnable{
	private final String fileDirectory;
	private final MongoClient mongoClient;
	private final String database;

	private static final Map<String,Long> readPosition = new ConcurrentHashMap<>();
	public static final Logger logger = LoggerFactory.getLogger(FileWatchTask.class);

	public FileWatchTask(String fileDirectory,String connectionString,String database) {
		this.fileDirectory = fileDirectory;
		this.mongoClient = MongoClients.create(connectionString);
		this.database = database;
	}


	public void run() {
		WatchService service = null;
		logger.info("Start listening");
		try {
			service = FileSystems.getDefault().newWatchService();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			Paths.get(fileDirectory).register(service, StandardWatchEventKinds.ENTRY_CREATE,
					StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
		} catch (IOException e) {
			e.printStackTrace();
		}
		while (true) {
			WatchKey key = null;
			try {
				key = service.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			for (WatchEvent<?> event : key.pollEvents()) {
				WatchEvent.Kind<?> kind = event.kind();

				if (kind == StandardWatchEventKinds.OVERFLOW) {
					logger.warn("Event overflow occurred.");
					continue;
				}
				WatchEvent<Path> ev = (WatchEvent<Path>) event;
				Path file = ev.context();
				String fileName = file.getFileName().toString();
				if(fileName.contains("skipErrorEvent")){
					logger.info("File modified:{}",file);
					if(fileName.contains("gz") || fileName.contains("zip")){
						readPosition.clear();
						logger.info("clear breakpoint");
					}else{
						readLogFile(file, readPosition.getOrDefault(fileName, 0L));
					}
				}
			}
			boolean valid = key.reset();
			if (!valid) {
				logger.warn("WatchKey no longer valid, exiting.");
				break;
			}
		}

	}

	private  void readLogFile(Path filePath,long lastReadPosition)  {
		try (RandomAccessFile raf = new RandomAccessFile(new File(fileDirectory+File.separator+filePath), "r")) {
			raf.seek(lastReadPosition);

			String line;
			StringBuilder last = new StringBuilder();
			while ((line = raf.readLine()) != null || last.length() != 0){
				String logLine = null;
				if(line != null){
					 logLine = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
				}
				processLog(logLine,last);
			}
			readPosition.put(filePath.getFileName().toString(),raf.getFilePointer());
		} catch (IOException e) {
			logger.warn("Failed to read file, file may be deleted");
			e.printStackTrace();
		}
	}

	private void processLog(String logLine,StringBuilder last) {
		if(last.length() == 0){
			last.append(logLine);
			return;
		}
		if(logLine == null){
			saveLog(last.toString());
			last.setLength(0);
		}else if((logLine.contains("[INFO ]") || logLine.contains("[WARN ]") || logLine.contains("[ERROR ]"))
				&&(last.toString().contains("[INFO ]") || last.toString().contains("[WARN ]") || last.toString().contains("[ERROR ]")) ){
			saveLog(last.toString());
			last.setLength(0);
			last.append(logLine);
		}else{
			last.append(logLine);
		}
	}

	private void saveLog(String logLine) {
		try{
			MongoCollection<Document> mongoCollection =mongoClient.getDatabase(database).getCollection("SkipEvent");
			GrokCompiler grokCompiler = GrokCompiler.newInstance();
			grokCompiler.register("DATETIME","[0-9,\\.\\-: ]+");
			grokCompiler.registerDefaultPatterns();
			Grok grok = grokCompiler.compile("\\[%{WORD:log_level} \\] %{DATETIME:dateTime} task-%{GREEDYDATA:taskId} skip event: %{NOTSPACE:event_class}: %{GREEDYDATA:event_data}");
			Match grokMatch = grok.match(logLine);
			Map<String, Object> resultMap = grokMatch.capture();
			logger.info(resultMap.toString());
			String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
			if(resultMap.get("dateTime") != null && resultMap.get("taskId") != null){
				Date date = simpleDateFormat.parse(resultMap.get("dateTime").toString());
				long timestamp = date.getTime();
				String eventId = resultMap.get("taskId")+Long.toString(timestamp);
				Document query = new Document("event_id", eventId);
				Document updateFields = new Document(resultMap);
				Document updateDocument = new Document("$set", updateFields);
				mongoCollection.updateOne(query,updateDocument,new UpdateOptions().upsert(true));
			}
		}catch (Exception e){
			logger.warn("Failed to save skip event,{}",e.getMessage());
		}

	}


}
