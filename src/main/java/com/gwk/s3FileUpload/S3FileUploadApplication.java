package com.gwk.s3FileUpload;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

//import java.io.File;

@SpringBootApplication
public class S3FileUploadApplication {
	public static void main(String[] args) throws IOException, InterruptedException {
		S3Client client = S3Client.builder()
				.region(Region.AP_NORTHEAST_2)
				.build();

		//register watch service on dir path.
		String dir = "D:/TEST";
		WatchService service = FileSystems.getDefault().newWatchService();
		Path path = Paths.get(dir);
		path.register(service,
				StandardWatchEventKinds.ENTRY_CREATE,
				StandardWatchEventKinds.ENTRY_DELETE,
				StandardWatchEventKinds.ENTRY_MODIFY);

		//create a file object with pathname
		//File myObj = new File("D:\\TEST\\test.txt");

		while(true) {
			WatchKey key = service.take();
			List<WatchEvent<?>> list = key.pollEvents();

			for(WatchEvent<?> event : list) {
				WatchEvent.Kind<?> kind = event.kind();
				Path pth = (Path) event.context();
				if (kind.equals(StandardWatchEventKinds.ENTRY_CREATE)) {
					System.out.println("생성 : " + pth.getFileName());
					String bucketName = "gunwoo-demo-datapipeline";
					String fileName = pth.toString();
					String filePath = "D:\\TEST\\" + fileName;
					PutObjectRequest request = PutObjectRequest.builder()
							.bucket(bucketName)
							.key(fileName)
							.build();
					System.out.println("!"+ filePath);
					Path testPath = Paths.get(filePath);
					client.putObject(request, RequestBody.fromFile(testPath));
					//client.putObject(request, RequestBody.fromFile(new File(filePath)));
					//client.putObject(request, RequestBody.fromFile(new File("D:\\TEST\\test.txt")));
					//client.putObject(request, RequestBody.fromFile(myObj));

				} else if (kind.equals(StandardWatchEventKinds.ENTRY_DELETE)) {
					System.out.println("삭제 : " + pth.getFileName());
				} else if (kind.equals(StandardWatchEventKinds.OVERFLOW)) {
					System.out.println("OVERFLOW");
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
