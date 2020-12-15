package com.amazonaws.samples;
/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.opencsv.CSVWriter;

import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on Amazon
 * S3, see http://aws.amazon.com/s3.
 * <p>
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (C:\\Users\\dilan\\.aws\\credentials) where the sample code will load the credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 *
 * http://aws.amazon.com/security-credentials
 */
public class UploadCsvToS3 {

	private static String pathRoot = System.getProperty("user.dir");

	public static void main(String[] args) throws IOException {

		Logger logger = getLogger();
		logger.info("Inicia...");


		//Carga settings de arch.properties
		Properties props = new Properties();
		//...


		FileInputStream fis = new FileInputStream(pathRoot+"\\settings.properties");
                InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
                props.load(isr);
		

		//Config aws
		String aws_region = props.getProperty("aws.Region");
		String aws_bucketName = props.getProperty("aws.bucketName");
		String aws_folderDest = props.getProperty("aws.folderDest");
		String aws_fileName = props.getProperty("aws.fileName");


		//Config bd
		String bd_host = props.getProperty("bd.host");
		String bd_port = props.getProperty("bd.port");
		String bd_name = props.getProperty("bd.name");
		String bd_user = props.getProperty("bd.user");
		String bd_pass = props.getProperty("bd.pass");

		//Query bd
		String query = props.getProperty("bd.query");


		//Nombre Archivo CSV
		//SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
		Date now = new Date();
		String strDate = sdf.format(now);
		String fileName_csv = aws_fileName+"_"+strDate+".csv";



		//BD
		String connectionUrl = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;user=%s;password=%s",bd_host, bd_port, bd_name, bd_user, bd_pass );
		String sql = null;
		try {
			try (Connection connection = DriverManager.getConnection(connectionUrl)) {
				logger.info("Connection BD done.");
				sql = query;
				try (
						Statement statement = connection.createStatement();
						ResultSet resultSet = statement.executeQuery(sql)) {
					logger.info("resultSet done.");
					//CREAR CSV
					convert_rs_to_csv(resultSet,fileName_csv);
					logger.info("create csv done.");
				}
				connection.close();
			}
		} catch (Exception e) {
			logger.warning("ERR BD:" + getStackTrace(e));
		}



		//AWS
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (C:\\Users\\dilan\\.aws\\credentials), and is in valid format.",
							e);
		}
		AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(aws_region)
				.build();

		//CARGA ARCHIVO AWS
		try {
			File file = new File(pathRoot +"\\"+fileName_csv);
			String key = aws_folderDest + file.getName();
			logger.info("Uploading a new object to S3 from a file");
			s3.putObject(new PutObjectRequest(aws_bucketName, key, file));
			logger.info("Uploading " + key + " OK...");
			file.delete();


		} catch (AmazonServiceException ase) {
			logger.warning("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			logger.warning("Error Message:    " + ase.getMessage());
			logger.warning("HTTP Status Code: " + ase.getStatusCode());
			logger.warning("AWS Error Code:   " + ase.getErrorCode());
			logger.warning("Error Type:       " + ase.getErrorType());
			logger.warning("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			logger.warning("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			logger.warning("Error Message: " + ace.getMessage());
		}
	}

	//CONVERT RS -> CSV
	private static void convert_rs_to_csv(ResultSet rs, String fileName) throws SQLException, IOException {
		char separator = ';';
		char quotechar = '\u0000';
		char escapechar = '"'; 	
		String lineEnd = "\n";

		CSVWriter writer = new CSVWriter(new FileWriter(fileName),separator,quotechar,escapechar,lineEnd);
		writer.writeAll(rs, true);
		writer.close();
	}

	//Set Logger
	private static Logger getLogger() throws SecurityException, IOException{
		Logger logger = Logger.getLogger("MyLogS3Sample");  
		FileHandler fh;  
		fh = new FileHandler( pathRoot+"/Log_"+getFecha_yyyyMMdd()+".log" , true);  
		SimpleFormatter formatter = new SimpleFormatter();
		fh.setFormatter(formatter);
		logger.addHandler(fh);
		return logger;
	}

	private static String getFecha_yyyyMMdd() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date now = new Date();
		return sdf.format(now);
	}

	private static String getStackTrace(Throwable t) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw, true);
		t.printStackTrace(pw);
		pw.flush();
		sw.flush();
		return sw.toString();
	}
}
