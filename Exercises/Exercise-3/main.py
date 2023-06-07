import boto3


def main():
    # Configure the AWS SDK

    import boto3
    import gzip

    # Create an S3 client
    s3 = boto3.client('s3')

    # S3 bucket and keys
    bucket_name = 'commoncrawl'
    index_key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'

    # Download the index file
    s3.download_file(bucket_name, index_key, 'wet.paths.gz')

    # Extract the file
    with gzip.open('wet.paths.gz', 'rt') as f:
        # Read the first line
        first_line = f.readline().strip()

    # Download the file from the URI in the first line
    s3.download_file(bucket_name, first_line, 'downloaded_file')

    # Iterate through the lines of the downloaded file
    with open('downloaded_file', 'r') as f:
        for line in f:
            print(line.strip())


    pass


if __name__ == "__main__":
    main()