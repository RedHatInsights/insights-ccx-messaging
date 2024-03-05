from s3_uploader import S3Uploader as IDPUploader
f= open("testfile.txt","a")
f.write("test")
lolo = {"access_key": "MMCXHNB6AQLUBLZ3ZXB1","secret_key":"MMCXHNB6AQLUBLZ3ZXB1","endpoint":"https://s3.upshift.redhat.com"}
uploader = IDPUploader(access_key="MMCXHNB6AQLUBLZ3ZXB1",secret_key="MMCXHNB6AQLUBLZ3ZXB1", endpoint="https://s3.upshift.redhat.com")
uploader.upload_file("testfile.txt","DH-QA-INSIGHTS-CCX","test/dsada")


from ccx_messaging.utils.s3_uploader import S3Uploader
ak = "MMCXHNB6AQLUBLZ3ZXB1"
sk = "MvvQxp6fXXNrT6HxAoO78BL4V3zTqwBCPLnBTBAP"
f= open("testfile","a")
f.write("test")
endpoint = "https://s3.upshift.redhat.com"
u = S3Uploader(access_key=ak, secret_key=sk, endpoint=endpoint)
u.upload_file("/testfile", "DH-QA-INSIGHTS-CCX", "test/hello")