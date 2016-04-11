package org.webdatacommons.structureddata.util;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.webdatacommons.structureddata.io.AsyncEntityWriter;
import org.webdatacommons.structureddata.model.Entity;
import org.webdatacommons.structureddata.model.EntityFileLoader;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import ldif.entity.NodeTrait;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;

/**
 * This class processes a number of input files and extracts different classes
 * of entities and writes them into separated files. The class file which is
 * needed has to follow the format: [CLASS]\t[NAMESCHEME]
 * 
 * Depending on the setup all entities from an URL are written to the file, if
 * the specific class appears on this URL or only the entity of the specific
 * class itself. In the first case, it might happen that quads/entities appear
 * in multiple files (meaning you create duplicates).
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
@Parameters(commandDescription = "Creates subsets of the original dataset, based on a given set of classes.")
public class SubsetCreator extends Processor<File> {

	@Parameter(names = { "-out",
			"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory;

	@Parameter(names = { "-in",
			"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;

	@Parameter(names = { "-p",
			"-prefix" }, description = "Prefix of files in the input folder which will be processed.")
	private String filePrefix = "";

	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;

	@Parameter(names = { "-cff",
			"classFilterFile" }, required = true, description = "File containing the class names, class prefixes and number of entities per file.")
	private String classFilterFile = null;

	@Parameter(names = "sep", required = false, description = "Separator for class filter file. (Default \t)")
	private String sep = "\t";

	private Map<String, AsyncEntityWriter> writer = new HashMap<String, AsyncEntityWriter>();

	@Override
	protected int getNumberOfThreads() {
		return this.threads;
	}

	@Override
	protected void beforeProcess() {
		try {
			BufferedReader br = InputUtil.getBufferedReader(new File(classFilterFile));
			while (br.ready()) {
				String line = br.readLine();
				String tok[] = line.split(sep);
				AsyncEntityWriter aWriter = new AsyncEntityWriter(new File(this.outputDirectory, tok[1] + ".gz"));
				aWriter.open();
				writer.put(tok[0], aWriter);
			}
			br.close();
		} catch (Exception e) {
			System.out.println("Could not read class filter file");
			e.printStackTrace();
			System.exit(0);
		}
	}

	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		Collections.sort(files);
		for (File f : inputDirectory.listFiles()) {
			if (!f.isDirectory()) {
				if (filePrefix.length() > 0) {
					if (!f.getName().startsWith(filePrefix)) {
						continue;
					}
				}
				files.add(f);
			}
		}
		return files;
	}

	@Override
	protected void process(File object) throws Exception {

		QuadFileLoader qfl = new QuadFileLoader();
		EntityFileLoader etl = new EntityFileLoader();
		BufferedReader br = InputUtil.getBufferedReader(object);
		String currentURL = "";
		NodeTrait currentSubject = null;
		List<Entity> entities = new ArrayList<Entity>();
		List<Quad> quads = new ArrayList<Quad>();
		while (br.ready()) {
			Quad q = qfl.parseQuadLine(br.readLine());
			if (q.graph().equals(currentURL)) {
				if (q.subject().equals(currentSubject)) {
					quads.add(q);
				} else {
					// create entity
					if (quads.size() > 0) {
						Entity e = etl.loadEntityFromQuads(quads);
						entities.add(e);
					}
					// clear list
					quads.clear();
					quads.add(q);
					currentSubject = q.subject();
				}
			} else {
				// create entity
				if (quads.size() > 0) {
					Entity e = etl.loadEntityFromQuads(quads);
					entities.add(e);
				}
				quads.clear();
				quads.add(q);
				currentSubject = q.subject();

				// create entity
				if (entities.size() > 0) {
					processEntities(entities);
				}
				// clear list
				entities.clear();
				currentURL = q.graph();
			}
		}
		// one final time:
		// create entity
		if (quads.size() > 0) {
			Entity e = etl.loadEntityFromQuads(quads);
			entities.add(e);
		}
		// create entity
		if (entities.size() > 0) {
			processEntities(entities);
		}
		br.close();
	}

	protected synchronized void writeEntitiesForTypes(Set<String> types, List<Entity> entities) {
		for (String type : types) {
			writer.get(type).append(entities);
		}
	}

	protected void processEntities(List<Entity> entities) {
		Set<String> types = new HashSet<String>();
		for (Entity e : entities) {
			if (e.getType() != null) {
				if (writer.containsKey(e.getType().value())) {
					types.add(e.getType().value());
				}
			}
		}
		writeEntitiesForTypes(types, entities);
	}

	@Override
	protected void afterProcess() {
		for (String s : writer.keySet()) {
			writer.get(s).close();
		}
	}

	public static void main(String[] args) {
		SubsetCreator cal = new SubsetCreator();
		try {
			new JCommander(cal, args);
			cal.process();
		} catch (ParameterException pe) {
			pe.printStackTrace();
			new JCommander(cal).usage();
		}
	}

}
