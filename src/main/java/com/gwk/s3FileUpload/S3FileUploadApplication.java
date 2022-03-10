package com.gwk.s3FileUpload;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

@SpringBootApplication
public class S3FileUploadApplication {
	public static void main(String[] args) throws IOException, InterruptedException {
		//to use this app, do cmd 'aws configure' and apply your access key and secret key.
		//you also can use application.yml.
		/*server:
		port: 8080
		cloud:
		aws:
		stack:
		auto: false
		region:
		static: ap-northeast-2 # AWS Region*/
		String args0;
		String root = "D:\\TEST";
		if (args.length != 0) {
			args0 = args[0];
			System.out.println(args0);
			args0 = args0.replaceAll("/", "\\");
			File file0 = new File(args0);
			if (file0.exists()) {
				System.out.println("route applied!");
				root = args0;
			}
		}

		//create s3 object
		S3Client client = S3Client.builder()
				.region(Region.AP_NORTHEAST_2)
				.build();
		//register watch service on root path. example of 'root' is an ML data storage folder.

		//set bucketName ... replace gunwoo-demo-datapipeline with your bucket name.
		String bucketName = "gunwoo-demo-datapipeline";


		WatchService service = FileSystems.getDefault().newWatchService();
		Path path = Paths.get(root);
		path.register(service,
				StandardWatchEventKinds.ENTRY_CREATE,
				StandardWatchEventKinds.ENTRY_DELETE,
				StandardWatchEventKinds.ENTRY_MODIFY);

		while(true) {
			WatchKey key = service.take();
			List<WatchEvent<?>> list = key.pollEvents();

			for(WatchEvent<?> event : list) {
				WatchEvent.Kind<?> kind = event.kind();
				Path pth = (Path) event.context();
				if (kind.equals(StandardWatchEventKinds.ENTRY_CREATE)) {
					//create event

					String fileName = pth.toString();
					String filePath = root + "\\" + fileName;
					PutObjectRequest request = PutObjectRequest.builder()
							.bucket(bucketName)
							.key(root + "/" + fileName)
							.build();

					Path testPath = Paths.get(filePath);
					try {
						client.putObject(request, RequestBody.fromFile(testPath));
					} catch (Exception e) {
						System.out.println("File uploading Failed.");
					}
				} else if (kind.equals(StandardWatchEventKinds.ENTRY_DELETE)) {
					//delete event
				} else if (kind.equals(StandardWatchEventKinds.OVERFLOW)) {
					//overflow event
				}
			}
			if (!key.reset()) {
				break;
			}
		}
		service.close();
	}
	//create bucket
	public static void createBucket(S3Client s3Client, String bucketName, Region region) {
		try {
			s3Client.createBucket(CreateBucketRequest
					.builder()
					.bucket(bucketName)
					.createBucketConfiguration(
							CreateBucketConfiguration.builder()
									.locationConstraint(region.id())
									.build())
					.build());
			System.out.println("Creating bucket: " + bucketName);
			s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
					.bucket(bucketName)
					.build());
			System.out.println(bucketName +" is ready.");
			System.out.printf("%n");
		} catch (S3Exception e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
	}
	//print bucketList
	public static void printBucketList(S3Client client) {
		ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
		ListBucketsResponse listBucketsResponse = client.listBuckets(listBucketsRequest);
		listBucketsResponse.buckets().stream().forEach(x -> System.out.println(x.name()));
	}
}
