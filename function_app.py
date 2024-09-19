import azure.functions as func
import logging
import uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import asyncio
import werkzeug.datastructures as dataStructures

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

acc_url = "ACCOUNT_URL"
acc_key = "ACCOUNT_KEY"

blob_service_client = BlobServiceClient(acc_url, credential= acc_key)

NUM_FILES = 1000

# New azure upload function
# Takes the local path to the file, the destination blob in Azure storage, the destination container in Azure storage
def azure_upload(file_object, destination_blob_name, destination_container_client):
    logging.info("Running azure_upload")
    
    # Check for blob
    blob_client = destination_container_client.get_blob_client(blob=destination_blob_name)
    blobExists = blob_client.exists()

    if not blobExists:
        logging.info("Uploading blob...")
        # Upload the local file
        with file_object as data:
            blob_client.upload_blob(data)
    else:
        logging.info("Blob already exists")

# new azure upload function to a container
async def upload_file_to_container(file, destination_blob_name, destination_container_client):
    logging.info("Running upload_file_to_container")
    loop = asyncio.get_event_loop()
    # save the file locally in the /tmp/filename
    file.save("/tmp/" + destination_blob_name)
    logging.info("Saved a file in upload_file_to_container")
    try:
        # take the file from the local folder then upload it
        await loop.run_in_executor(None, azure_upload, open("/tmp/" + destination_blob_name, 'rb'), destination_blob_name, destination_container_client)
    except Exception as e:
        logging.info(f"Exception occured: {e}")

# new upload files to azure container function
async def upload_files_to_container(files, filenames, destination_container_client):
    logging.info("Running upload_fileSSS_to_container")
    await asyncio.gather(*[upload_file_to_container(file, filename, destination_container_client) for file, filename in
                        zip(files, filenames)])

# new count files with name function for azure storage
# counts the number of files with the given name
# "user" is misleading because it is actually the full file name
# The commented line was used to put a '/' between the hash and the actual file name
# However, because Azure does not default to hierarchal structuring, this will not work
# Instead, we create a new container using the hash in "upload"
# The hash is also truncated before uploading in "upload"
def count_files_with_name(user, destination_container_client):
    name = list(user)
    #name[8] = "/"
    name = "".join(name)
    logging.info(f"Looking for files starting with: {name}")
    return len(list(destination_container_client.list_blobs(name_starts_with=name)))

# new count files with name async function for azure storage ( no changes were needed )
async def count_files_with_names(users, destination_container_client):
    loop = asyncio.get_event_loop()
    return await asyncio.gather(*[loop.run_in_executor(None, count_files_with_name, user, destination_container_client) for user in users])

# new count files function for azure storage ( no changes were needed )
def count_files(filenames, destination_container_client):
    counts = asyncio.run(count_files_with_names(filenames, destination_container_client))
    missed = []

    for fname, count in zip(filenames, counts):
        if count == 0:
            missed.append(fname)
    
    if not missed:
        return True
    return False

# original upload function for azure storage
@app.function_name(name="upload")
@app.route(route="upload")
def upload(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == 'POST':

        # Retrieve files
        files = []
        for i in range(1, NUM_FILES + 1):
            file = req.files.get(f'file{i}', None)
            if not file: break
            files.append(file)

        # Extract filenames and filepaths to save to
        # trim the hash off the filename
        filenames = [file.filename[9:] for file in files]
        logging.info("File count: " + str(filenames))

        # Get the container name from the hash prefix
        container_name = ""
        if (files != None and len(files) > 0 and len(files[0].filename) > 7):
            name = list(files[0].filename)[:8]
            name = "".join(name)
            container_name = name
        else:
            return func.HttpResponse(body="No files to upload or bad file names", status_code=501)
        
        # Get the container
        container_client = blob_service_client.get_container_client(container_name)
        containerExists = container_client.exists()

        # Open or create the container
        if not containerExists:
            container_client = blob_service_client.create_container(name=container_name)
            logging.info("Creating container")
        else:
            logging.info("Container already exists")

        # Try uploading the files to the bucket
        try:
            asyncio.run(upload_files_to_container(files, filenames, container_client))
            if count_files(filenames, container_client):
                return func.HttpResponse(body=f'{len(filenames)} files uploaded', status_code=201)
            else:
                return func.HttpResponse(body="Count failed", status_code=500)

        except Exception as e:
            logging.info("Encountered exception: " + str(e))
            return func.HttpResponse(body="Exception", status_code=400)

    else:
        return func.HttpResponse(body="Only Post method allowed", status_code=405)
